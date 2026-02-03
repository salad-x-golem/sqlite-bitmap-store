package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/salad-x-golem/sqlite-bitmap-store/store"

	arkivevents "github.com/salad-x-golem/arkiv-events"
	"github.com/salad-x-golem/arkiv-events/events"
)

var (
	// Metrics for tracking operations
	metricOperationStarted    = metrics.NewRegisteredCounter("arkiv_store/operations_started", nil)
	metricOperationSuccessful = metrics.NewRegisteredCounter("arkiv_store/operations_successful", nil)
	metricCreates             = metrics.NewRegisteredMeter("arkiv_store/creates", nil)
	metricUpdates             = metrics.NewRegisteredMeter("arkiv_store/updates", nil)
	metricDeletes             = metrics.NewRegisteredMeter("arkiv_store/deletes", nil)
	metricExtends             = metrics.NewRegisteredMeter("arkiv_store/extends", nil)
	metricOwnerChanges        = metrics.NewRegisteredMeter("arkiv_store/owner_changes", nil)
	// more responsive
	metricOperationTime = metrics.NewRegisteredHistogram("arkiv_store/operation_time_ms", nil, metrics.NewExpDecaySample(100, 0.4))
)

type SQLiteStore struct {
	writePool *sql.DB
	readPool  *sql.DB
	log       *slog.Logger
}

func NewSQLiteStore(
	log *slog.Logger,
	dbPath string,
	numberOfReadThreads int,
) (*SQLiteStore, error) {

	err := os.MkdirAll(filepath.Dir(dbPath), 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	writeURL := fmt.Sprintf("file:%s?mode=rwc&_busy_timeout=11000&_journal_mode=WAL&_auto_vacuum=incremental&_foreign_keys=true&_txlock=immediate&_cache_size=65536", dbPath)

	writePool, err := sql.Open("sqlite3", writeURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open write pool: %w", err)
	}

	readURL := fmt.Sprintf("file:%s?_query_only=true&_busy_timeout=11000&_journal_mode=WAL&_auto_vacuum=incremental&_foreign_keys=true&_txlock=deferred&_cache_size=65536", dbPath)
	readPool, err := sql.Open("sqlite3", readURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open read pool: %w", err)
	}

	readPool.SetMaxOpenConns(numberOfReadThreads)
	readPool.SetMaxIdleConns(numberOfReadThreads)
	readPool.SetConnMaxLifetime(0)
	readPool.SetConnMaxIdleTime(0)

	err = runMigrations(writePool)
	if err != nil {
		writePool.Close()
		readPool.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &SQLiteStore{writePool: writePool, readPool: readPool, log: log}, nil
}

func runMigrations(db *sql.DB) error {
	sourceDriver, err := iofs.New(store.Migrations, "schema")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	dbDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", dbDriver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

func (s *SQLiteStore) Close() error {
	return s.writePool.Close()
}

func (s *SQLiteStore) GetLastBlock(ctx context.Context) (uint64, error) {
	return store.New(s.writePool).GetLastBlock(ctx)
}

type blockStats struct {
	creates      int
	updates      int
	deletes      int
	extends      int
	ownerChanges int
}

func (s *SQLiteStore) FollowEvents(ctx context.Context, iterator arkivevents.BatchIterator) error {

	for batch := range iterator {
		if batch.Error != nil {
			return fmt.Errorf("failed to follow events: %w", batch.Error)
		}

		// We will calculate totals for the log at the end, but track per-block for metrics
		stats := make(map[uint64]*blockStats)

		err := func() error {

			tx, err := s.writePool.BeginTx(ctx, &sql.TxOptions{
				Isolation: sql.LevelSerializable,
				ReadOnly:  false,
			})
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			st := store.New(tx)

			firstBlock := batch.Batch.Blocks[0].Number
			lastBlock := batch.Batch.Blocks[len(batch.Batch.Blocks)-1].Number
			s.log.Info("new batch", "firstBlock", firstBlock, "lastBlock", lastBlock)

			lastBlockFromDB, err := st.GetLastBlock(ctx)
			if err != nil {
				return fmt.Errorf("failed to get last block from database: %w", err)
			}

			cache := newBitmapCache(st)

			startTime := time.Now()

			metricOperationStarted.Inc(1)

		mainLoop:
			for _, block := range batch.Batch.Blocks {

				if block.Number <= uint64(lastBlockFromDB) {
					s.log.Info("skipping block", "block", block.Number, "lastBlockFromDB", lastBlockFromDB)
					continue mainLoop
				}

				// Initialize stats for this block
				if _, ok := stats[block.Number]; !ok {
					stats[block.Number] = &blockStats{}
				}
				blockStat := stats[block.Number]

				updatesMap := map[common.Hash][]*events.OPUpdate{}

				for _, operation := range block.Operations {
					if operation.Update != nil {
						currentUpdates := updatesMap[operation.Update.Key]
						currentUpdates = append(currentUpdates, operation.Update)
						updatesMap[operation.Update.Key] = currentUpdates
					}
				}

			operationLoop:
				for _, operation := range block.Operations {

					switch {

					case operation.Create != nil:
						// expiresAtBlock := blockNumber + operation.Create.BTL
						blockStat.creates++
						key := operation.Create.Key

						stringAttributes := maps.Clone(operation.Create.StringAttributes)

						stringAttributes["$owner"] = strings.ToLower(operation.Create.Owner.Hex())
						stringAttributes["$creator"] = strings.ToLower(operation.Create.Owner.Hex())
						stringAttributes["$key"] = strings.ToLower(key.Hex())

						untilBlock := block.Number + operation.Create.BTL
						numericAttributes := maps.Clone(operation.Create.NumericAttributes)
						numericAttributes["$expiration"] = uint64(untilBlock)
						numericAttributes["$createdAtBlock"] = uint64(block.Number)
						numericAttributes["$lastModifiedAtBlock"] = uint64(block.Number)

						sequence := block.Number<<32 | operation.TxIndex<<16 | operation.OpIndex
						numericAttributes["$sequence"] = sequence
						numericAttributes["$txIndex"] = uint64(operation.TxIndex)
						numericAttributes["$opIndex"] = uint64(operation.OpIndex)

						id, err := st.UpsertPayload(
							ctx,
							store.UpsertPayloadParams{
								EntityKey:         operation.Create.Key.Bytes(),
								Payload:           operation.Create.Content,
								ContentType:       operation.Create.ContentType,
								StringAttributes:  store.NewStringAttributes(stringAttributes),
								NumericAttributes: store.NewNumericAttributes(numericAttributes),
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload %s at block %d txIndex %d opIndex %d: %w", key.Hex(), block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						for k, v := range stringAttributes {
							err = cache.AddToStringBitmap(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add string attribute value bitmap: %w", err)
							}
						}

						for k, v := range numericAttributes {

							// skip txIndex and opIndex because they are not used for querying
							switch k {
							case "$txIndex", "$opIndex":
								continue
							}

							err = cache.AddToNumericBitmap(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
							}
						}
					case operation.Update != nil:
						blockStat.updates++

						updates := updatesMap[operation.Update.Key]
						lastUpdate := updates[len(updates)-1]

						if operation.Update != lastUpdate {
							continue operationLoop
						}

						key := operation.Update.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldStringAttributes := latestPayload.StringAttributes

						oldNumericAttributes := latestPayload.NumericAttributes

						stringAttributes := maps.Clone(operation.Update.StringAttributes)

						stringAttributes["$owner"] = strings.ToLower(operation.Update.Owner.Hex())
						stringAttributes["$creator"] = oldStringAttributes.Values["$creator"]
						stringAttributes["$key"] = strings.ToLower(operation.Update.Key.Hex())

						untilBlock := block.Number + operation.Update.BTL
						numericAttributes := maps.Clone(operation.Update.NumericAttributes)
						numericAttributes["$expiration"] = uint64(untilBlock)
						numericAttributes["$createdAtBlock"] = oldNumericAttributes.Values["$createdAtBlock"]

						numericAttributes["$sequence"] = oldNumericAttributes.Values["$sequence"]
						numericAttributes["$txIndex"] = oldNumericAttributes.Values["$txIndex"]
						numericAttributes["$opIndex"] = oldNumericAttributes.Values["$opIndex"]
						numericAttributes["$lastModifiedAtBlock"] = uint64(block.Number)

						id, err := st.UpsertPayload(
							ctx,
							store.UpsertPayloadParams{
								EntityKey:         key,
								Payload:           operation.Update.Content,
								ContentType:       operation.Update.ContentType,
								StringAttributes:  store.NewStringAttributes(stringAttributes),
								NumericAttributes: store.NewNumericAttributes(numericAttributes),
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload 0x%x at block %d txIndex %d opIndex %d: %w", key, block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						for k, v := range oldStringAttributes.Values {
							err = cache.RemoveFromStringBitmap(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to remove string attribute value bitmap: %w", err)
							}
						}

						for k, v := range oldNumericAttributes.Values {
							// skip txIndex and opIndex because they are not used for querying
							switch k {
							case "$txIndex", "$opIndex":
								continue
							}

							err = cache.RemoveFromNumericBitmap(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
							}
						}

						// TODO: delete entity from the indexes

						for k, v := range stringAttributes {
							err = cache.AddToStringBitmap(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add string attribute value bitmap: %w", err)
							}
						}

						for k, v := range numericAttributes {
							// skip txIndex and opIndex because they are not used for querying
							switch k {
							case "$txIndex", "$opIndex":
								continue
							}

							err = cache.AddToNumericBitmap(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
							}
						}

					case operation.Delete != nil || operation.Expire != nil:

						blockStat.deletes++
						var key []byte
						if operation.Delete != nil {
							key = common.Hash(*operation.Delete).Bytes()
						} else {
							key = common.Hash(*operation.Expire).Bytes()
						}

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldStringAttributes := latestPayload.StringAttributes

						oldNumericAttributes := latestPayload.NumericAttributes

						for k, v := range oldStringAttributes.Values {
							err = cache.RemoveFromStringBitmap(ctx, k, v, latestPayload.ID)
							if err != nil {
								return fmt.Errorf("failed to remove string attribute value bitmap: %w", err)
							}
						}

						for k, v := range oldNumericAttributes.Values {
							// skip txIndex and opIndex because they are not used for querying
							switch k {
							case "$txIndex", "$opIndex":
								continue
							}

							err = cache.RemoveFromNumericBitmap(ctx, k, v, latestPayload.ID)
							if err != nil {
								return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
							}
						}

						err = st.DeletePayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to delete payload: %w", err)
						}

					case operation.ExtendBTL != nil:

						blockStat.extends++

						key := operation.ExtendBTL.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldNumericAttributes := latestPayload.NumericAttributes

						newToBlock := block.Number + operation.ExtendBTL.BTL

						numericAttributes := maps.Clone(oldNumericAttributes.Values)
						numericAttributes["$expiration"] = uint64(newToBlock)

						oldExpiration := oldNumericAttributes.Values["$expiration"]

						id, err := st.UpsertPayload(ctx, store.UpsertPayloadParams{
							EntityKey:         key,
							Payload:           latestPayload.Payload,
							ContentType:       latestPayload.ContentType,
							StringAttributes:  latestPayload.StringAttributes,
							NumericAttributes: store.NewNumericAttributes(numericAttributes),
						})
						if err != nil {
							return fmt.Errorf("failed to insert payload at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = cache.RemoveFromNumericBitmap(ctx, "$expiration", oldExpiration, id)
						if err != nil {
							return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
						}

						err = cache.AddToNumericBitmap(ctx, "$expiration", newToBlock, id)
						if err != nil {
							return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
						}

					case operation.ChangeOwner != nil:
						blockStat.ownerChanges++
						key := operation.ChangeOwner.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						stringAttributes := latestPayload.StringAttributes

						oldOwner := stringAttributes.Values["$owner"]

						newOwner := strings.ToLower(operation.ChangeOwner.Owner.Hex())

						stringAttributes.Values["$owner"] = newOwner

						id, err := st.UpsertPayload(
							ctx,
							store.UpsertPayloadParams{
								EntityKey:         key,
								Payload:           latestPayload.Payload,
								ContentType:       latestPayload.ContentType,
								StringAttributes:  stringAttributes,
								NumericAttributes: latestPayload.NumericAttributes,
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = cache.RemoveFromStringBitmap(ctx, "$owner", oldOwner, id)
						if err != nil {
							return fmt.Errorf("failed to remove string attribute value bitmap for owner: %w", err)
						}

						err = cache.AddToStringBitmap(ctx, "$owner", newOwner, id)
						if err != nil {
							return fmt.Errorf("failed to add string attribute value bitmap for owner: %w", err)
						}

					default:
						return fmt.Errorf("unknown operation: %v", operation)
					}

				}

				// Log per block if needed, but we can now rely on the map for totals later
				s.log.Info("block updated", "block", block.Number, "creates", blockStat.creates, "updates", blockStat.updates, "deletes", blockStat.deletes, "extends", blockStat.extends, "ownerChanges", blockStat.ownerChanges)
			}

			err = st.UpsertLastBlock(ctx, lastBlock)
			if err != nil {
				return fmt.Errorf("failed to upsert last block: %w", err)
			}

			err = cache.Flush(ctx)
			if err != nil {
				return fmt.Errorf("failed to flush bitmap cache: %w", err)
			}

			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			// Calculate batch totals for logging and update metrics PER BLOCK
			var (
				totalCreates      int
				totalUpdates      int
				totalDeletes      int
				totalExtends      int
				totalOwnerChanges int
			)

			// Iterate blocks again to preserve order and update metrics per block
			for _, block := range batch.Batch.Blocks {
				if s, ok := stats[block.Number]; ok {
					totalCreates += s.creates
					totalUpdates += s.updates
					totalDeletes += s.deletes
					totalExtends += s.extends
					totalOwnerChanges += s.ownerChanges

					// Update metrics specifically per block
					if s.creates > 0 {
						metricCreates.Mark(int64(s.creates))
					}
					if s.updates > 0 {
						metricUpdates.Mark(int64(s.updates))
					}
					if s.deletes > 0 {
						metricDeletes.Mark(int64(s.deletes))
					}
					if s.extends > 0 {
						metricExtends.Mark(int64(s.extends))
					}
					if s.ownerChanges > 0 {
						metricOwnerChanges.Mark(int64(s.ownerChanges))
					}
				}
			}

			metricOperationSuccessful.Inc(1)
			metricOperationTime.Update(time.Since(startTime).Milliseconds())

			s.log.Info("batch processed",
				"firstBlock", firstBlock,
				"lastBlock", lastBlock,
				"processingTime", time.Since(startTime).Milliseconds(),
				"creates", totalCreates,
				"updates", totalUpdates,
				"deletes", totalDeletes,
				"extends", totalExtends,
				"ownerChanges", totalOwnerChanges)

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLiteStore) NewQueries() *store.Queries {
	return store.New(s.readPool)
}

func (s *SQLiteStore) ReadTransaction(ctx context.Context, fn func(q *store.Queries) error) error {
	tx, err := s.readPool.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	st := store.New(tx)

	return fn(st)
}

func (s *SQLiteStore) GetNumberOfEntities(ctx context.Context) (numberOfEntities uint64, err error) {
	err = s.ReadTransaction(ctx, func(q *store.Queries) error {
		ni, err := q.GetNumberOfEntities(ctx)
		if err != nil {
			return fmt.Errorf("failed to get number of entities: %w", err)
		}
		numberOfEntities = uint64(ni)
		return nil
	})

	if err != nil {
		return 0, err
	}

	return numberOfEntities, nil
}
