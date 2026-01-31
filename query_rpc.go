package sqlitebitmapstore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/salad-x-golem/sqlite-bitmap-store/query"
	"github.com/salad-x-golem/sqlite-bitmap-store/store"
)

const QueryResultCountLimit uint64 = 200

type IncludeData struct {
	Key                         bool `json:"key"`
	Attributes                  bool `json:"attributes"`
	SyntheticAttributes         bool `json:"syntheticAttributes"`
	Payload                     bool `json:"payload"`
	ContentType                 bool `json:"contentType"`
	Expiration                  bool `json:"expiration"`
	Owner                       bool `json:"owner"`
	CreatedAtBlock              bool `json:"createdAtBlock"`
	LastModifiedAtBlock         bool `json:"lastModifiedAtBlock"`
	TransactionIndexInBlock     bool `json:"transactionIndexInBlock"`
	OperationIndexInTransaction bool `json:"operationIndexInTransaction"`
}

type Options struct {
	AtBlock        *uint64      `json:"atBlock,omitempty"`
	IncludeData    *IncludeData `json:"includeData,omitempty"`
	ResultsPerPage *uint64      `json:"resultsPerPage,omitempty"`
	Cursor         string       `json:"cursor,omitempty"`
}

func (o *Options) GetAtBlock() uint64 {
	if o == nil || o.AtBlock == nil {
		return 0
	}
	return *o.AtBlock
}

func (o *Options) GetResultsPerPage() uint64 {
	if o == nil || o.ResultsPerPage == nil || *o.ResultsPerPage > QueryResultCountLimit {
		return QueryResultCountLimit
	}
	return *o.ResultsPerPage
}

func (o *Options) GetIncludeData() IncludeData {
	if o == nil || o.IncludeData == nil {
		return IncludeData{
			Key:         true,
			ContentType: true,
			Payload:     true,
			Owner:       true,
			Attributes:  true,
			Expiration:  true,
		}
	}
	return *o.IncludeData
}

func (o *Options) GetCursor() (*uint64, error) {
	if o == nil || o.Cursor == "" {
		return nil, nil
	}

	cursor, err := hexutil.DecodeUint64(o.Cursor)
	if err != nil {
		return nil, fmt.Errorf("error decoding cursor: %w", err)
	}

	return &cursor, nil
}

type QueryResponse struct {
	Data        []json.RawMessage `json:"data"`
	BlockNumber uint64            `json:"blockNumber"`
	Cursor      *string           `json:"cursor,omitempty"`
}

type EntityData struct {
	Key                         *common.Hash    `json:"key,omitempty"`
	Value                       hexutil.Bytes   `json:"value,omitempty"`
	ContentType                 *string         `json:"contentType,omitempty"`
	ExpiresAt                   *uint64         `json:"expiresAt,omitempty"`
	Owner                       *common.Address `json:"owner,omitempty"`
	CreatedAtBlock              *uint64         `json:"createdAtBlock,omitempty"`
	LastModifiedAtBlock         *uint64         `json:"lastModifiedAtBlock,omitempty"`
	TransactionIndexInBlock     *uint64         `json:"transactionIndexInBlock,omitempty"`
	OperationIndexInTransaction *uint64         `json:"operationIndexInTransaction,omitempty"`

	StringAttributes  []Attribute[string] `json:"stringAttributes,omitempty"`
	NumericAttributes []Attribute[uint64] `json:"numericAttributes,omitempty"`
}

type Attribute[T any] struct {
	Key   string `json:"key"`
	Value T      `json:"value"`
}

const maxResultBytes = 512 * 1024 * 1024

func (s *SQLiteStore) QueryEntities(
	ctx context.Context,
	queryStr string,
	options *Options,
) (*QueryResponse, error) {

	// TODO: wait for the block height

	res := &QueryResponse{
		Data:        []json.RawMessage{},
		BlockNumber: 0,
		Cursor:      nil,
	}

	{
		q := s.NewQueries()
		timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		for {
			lastBlock, err := q.GetLastBlock(ctx)
			if err != nil {
				return nil, fmt.Errorf("error getting last block: %w", err)
			}
			if lastBlock >= options.GetAtBlock() {
				break
			}
			select {
			case <-timeoutCtx.Done():
				return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		cancel()
	}

	q, err := query.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing query: %w", err)
	}

	err = s.ReadTransaction(ctx, func(queries *store.Queries) error {

		bitmap, err := q.Evaluate(
			ctx,
			queries,
		)
		if err != nil {
			return fmt.Errorf("error evaluating query: %w", err)
		}

		cursor, err := options.GetCursor()
		if err != nil {
			return fmt.Errorf("error decoding cursor: %w", err)
		}

		// The cursor contains the last value that was included in the previous page.
		// We create a bitmask by creating an empty bitmap, and then flipping the bits
		// from 0 to (cursor - 1) to 1, so that we only include values below the cursor
		// value.
		if cursor != nil {
			s.log.Info("decoded cursor", "value", *cursor)
			cursorMask := roaring64.New()
			cursorMask.AddRange(0, *cursor)
			bitmap.And(cursorMask)
		}

		it := bitmap.ReverseIterator()

		maxResults := options.GetResultsPerPage()

		nextIDs := func(max uint64) []uint64 {
			ids := []uint64{}
			for range max {
				if !it.HasNext() {
					break
				}
				ids = append(ids, it.Next())
			}
			return ids
		}

		totalBytes := uint64(0)
		finished := true
		var lastID *uint64

	fillLoop:
		for it.HasNext() {

			nextBatchSize := min(maxResults-uint64(len(res.Data)), 10)

			nextIDs := nextIDs(nextBatchSize)

			payloads, err := queries.RetrievePayloads(ctx, nextIDs)
			if err != nil {
				return fmt.Errorf("error retrieving payloads: %w", err)
			}

			for _, payload := range payloads {

				lastID = &payload.ID

				ed := toPayload(payload, options.GetIncludeData())
				d, err := json.Marshal(ed)
				if err != nil {
					return fmt.Errorf("error marshalling entity data: %w", err)
				}
				res.Data = append(res.Data, d)
				totalBytes += uint64(len(d))

				if totalBytes > maxResultBytes {
					finished = false
					break fillLoop
				}

				if uint64(len(res.Data)) >= maxResults {
					finished = false
					break fillLoop
				}

			}

		}

		if !finished {
			res.Cursor = pointerOf(hexutil.EncodeUint64(*lastID))
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error peforming query: %w", err)
	}

	return res, nil

}

func pointerOf[T any](v T) *T {
	return &v
}

func filterAttributes[T any](predicate func(string) bool, m map[string]T) []Attribute[T] {
	res := []Attribute[T]{}

	for k, v := range m {
		if !predicate(k) {
			continue
		}
		res = append(res, Attribute[T]{Key: k, Value: v})
	}

	slices.SortFunc(res, func(i, j Attribute[T]) int {
		return strings.Compare(i.Key, j.Key)
	})

	return res
}

func syntheticPredicate(k string) bool {
	return strings.HasPrefix(k, "$")
}

func nonSyntheticPredicate(k string) bool {
	return !strings.HasPrefix(k, "$")
}

func anyPredicate(k string) bool {
	return true
}

func toPayload(r store.RetrievePayloadsRow, includeData IncludeData) *EntityData {
	res := &EntityData{}
	if includeData.Key {
		res.Key = pointerOf(common.BytesToHash(r.EntityKey))
	}
	if includeData.Payload {
		res.Value = r.Payload
	}

	if includeData.ContentType {
		res.ContentType = &r.ContentType
	}

	switch {
	case includeData.Attributes && includeData.SyntheticAttributes:
		res.StringAttributes = filterAttributes(anyPredicate, r.StringAttributes.Values)
		res.NumericAttributes = filterAttributes(anyPredicate, r.NumericAttributes.Values)
	case includeData.Attributes:
		res.StringAttributes = filterAttributes(nonSyntheticPredicate, r.StringAttributes.Values)
		res.NumericAttributes = filterAttributes(nonSyntheticPredicate, r.NumericAttributes.Values)
	case includeData.SyntheticAttributes:
		res.StringAttributes = filterAttributes(syntheticPredicate, r.StringAttributes.Values)
		res.NumericAttributes = filterAttributes(syntheticPredicate, r.NumericAttributes.Values)
	}

	if includeData.Expiration {
		res.ExpiresAt = pointerOf(r.NumericAttributes.Values["$expiration"])
	}

	if includeData.Owner {
		res.Owner = pointerOf(common.HexToAddress(r.StringAttributes.Values["$owner"]))
	}

	if includeData.CreatedAtBlock {
		res.CreatedAtBlock = pointerOf(r.NumericAttributes.Values["$createdAtBlock"])
	}

	if includeData.LastModifiedAtBlock {
		res.LastModifiedAtBlock = pointerOf(r.NumericAttributes.Values["$lastModifiedAtBlock"])
	}

	if includeData.TransactionIndexInBlock {
		res.TransactionIndexInBlock = pointerOf(r.NumericAttributes.Values["$txIndex"])
	}

	if includeData.OperationIndexInTransaction {
		res.OperationIndexInTransaction = pointerOf(r.NumericAttributes.Values["$opIndex"])
	}

	return res

}
