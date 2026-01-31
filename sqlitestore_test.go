package sqlitebitmapstore_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	arkivevents "github.com/salad-x-golem/arkiv-events"
	"github.com/salad-x-golem/arkiv-events/events"
	sqlitebitmapstore "github.com/salad-x-golem/sqlite-bitmap-store"
	"github.com/salad-x-golem/sqlite-bitmap-store/pusher"
	"github.com/salad-x-golem/sqlite-bitmap-store/store"
)

var _ = Describe("SQLiteStore", func() {
	var (
		sqlStore *sqlitebitmapstore.SQLiteStore
		tmpDir   string
		ctx      context.Context
		cancel   context.CancelFunc
		logger   *slog.Logger
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "sqlitestore_test")
		Expect(err).NotTo(HaveOccurred())

		logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))
		dbPath := filepath.Join(tmpDir, "test.db")

		sqlStore, err = sqlitebitmapstore.NewSQLiteStore(logger, dbPath, 4)
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
		if sqlStore != nil {
			sqlStore.Close()
		}
		os.RemoveAll(tmpDir)
	})

	Describe("FollowEvents with batch of two blocks", func() {
		It("should insert data and allow querying by string and numeric attributes", func() {
			iterator := pusher.NewPushIterator()

			key1 := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
			key2 := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			batch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key1,
									ContentType: "application/json",
									BTL:         1000,
									Owner:       owner,
									Content:     []byte(`{"name": "document1"}`),
									StringAttributes: map[string]string{
										"type":     "document",
										"category": "reports",
									},
									NumericAttributes: map[string]uint64{
										"version":  1,
										"priority": 10,
									},
								},
							},
						},
					},
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key2,
									ContentType: "image/png",
									BTL:         2000,
									Owner:       owner,
									Content:     []byte(`image data`),
									StringAttributes: map[string]string{
										"type":     "image",
										"category": "media",
									},
									NumericAttributes: map[string]uint64{
										"version":  2,
										"priority": 20,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				iterator.Push(ctx, batch)
				iterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(iterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			lastBlock, err := sqlStore.GetLastBlock(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(lastBlock).To(Equal(uint64(101)))

			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				// Query by string attribute: type = "document"
				docBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "type",
					Value: "document",
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(docBitmap).NotTo(BeNil())

				docIDs := docBitmap.ToArray()
				Expect(docIDs).To(HaveLen(1))

				docPayloads, err := q.RetrievePayloads(ctx, docIDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(docPayloads).To(HaveLen(1))
				Expect(docPayloads[0].Payload).To(Equal([]byte(`{"name": "document1"}`)))
				Expect(docPayloads[0].ContentType).To(Equal("application/json"))
				Expect(docPayloads[0].StringAttributes.Values["type"]).To(Equal("document"))

				// Query by string attribute: type = "image"
				imageBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "type",
					Value: "image",
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(imageBitmap).NotTo(BeNil())

				imageIDs := imageBitmap.ToArray()
				Expect(imageIDs).To(HaveLen(1))

				imagePayloads, err := q.RetrievePayloads(ctx, imageIDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(imagePayloads).To(HaveLen(1))
				Expect(imagePayloads[0].Payload).To(Equal([]byte(`image data`)))
				Expect(imagePayloads[0].ContentType).To(Equal("image/png"))

				// Query by numeric attribute: version = 1
				version1Bitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
					Name:  "version",
					Value: 1,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(version1Bitmap).NotTo(BeNil())

				version1IDs := version1Bitmap.ToArray()
				Expect(version1IDs).To(HaveLen(1))

				version1Payloads, err := q.RetrievePayloads(ctx, version1IDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(version1Payloads).To(HaveLen(1))
				Expect(version1Payloads[0].NumericAttributes.Values["version"]).To(Equal(uint64(1)))

				// Query by numeric attribute: version > 1
				versionGT1Bitmaps, err := q.EvaluateNumericAttributeValueGreaterThan(ctx, store.EvaluateNumericAttributeValueGreaterThanParams{
					Name:  "version",
					Value: 1,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(versionGT1Bitmaps).To(HaveLen(1))

				// Combine bitmaps to get all IDs with version > 1
				combinedBitmap := store.NewBitmap()
				for _, bm := range versionGT1Bitmaps {
					combinedBitmap.Or(bm.Bitmap)
				}

				versionGT1IDs := combinedBitmap.ToArray()
				Expect(versionGT1IDs).To(HaveLen(1))

				versionGT1Payloads, err := q.RetrievePayloads(ctx, versionGT1IDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(versionGT1Payloads).To(HaveLen(1))
				Expect(versionGT1Payloads[0].NumericAttributes.Values["version"]).To(Equal(uint64(2)))

				// Query by numeric attribute: priority >= 10
				priorityGTE10Bitmaps, err := q.EvaluateNumericAttributeValueGreaterOrEqualThan(ctx, store.EvaluateNumericAttributeValueGreaterOrEqualThanParams{
					Name:  "priority",
					Value: 10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(priorityGTE10Bitmaps).To(HaveLen(2))

				priorityCombined := store.NewBitmap()
				for _, bm := range priorityGTE10Bitmaps {
					priorityCombined.Or(bm.Bitmap)
				}

				priorityIDs := priorityCombined.ToArray()
				Expect(priorityIDs).To(HaveLen(2))

				priorityPayloads, err := q.RetrievePayloads(ctx, priorityIDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(priorityPayloads).To(HaveLen(2))

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents Update operation", func() {
		It("should update payload and bitmap indexes correctly", func() {
			key := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("original content"),
									StringAttributes: map[string]string{
										"status": "draft",
									},
									NumericAttributes: map[string]uint64{
										"version": 1,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Now update the entity
			updateIterator := pusher.NewPushIterator()
			updateBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "application/json",
									BTL:         1000,
									Owner:       owner,
									Content:     []byte(`{"updated": true}`),
									StringAttributes: map[string]string{
										"status": "published",
									},
									NumericAttributes: map[string]uint64{
										"version": 2,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				updateIterator.Push(ctx, updateBatch)
				updateIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(updateIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify the update
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())

				Expect(row.Payload).To(Equal([]byte(`{"updated": true}`)))
				Expect(row.ContentType).To(Equal("application/json"))
				Expect(row.StringAttributes.Values["status"]).To(Equal("published"))
				Expect(row.NumericAttributes.Values["version"]).To(Equal(uint64(2)))
				Expect(row.NumericAttributes.Values["$lastModifiedAtBlock"]).To(Equal(uint64(101)))
				// $createdAtBlock should be preserved
				Expect(row.NumericAttributes.Values["$createdAtBlock"]).To(Equal(uint64(100)))

				// Verify old bitmap index is removed
				oldStatusBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "status",
					Value: "draft",
				})
				Expect(err).To(HaveOccurred()) // Should not find old value

				// Verify new bitmap index exists
				newStatusBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "status",
					Value: "published",
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(newStatusBitmap.ToArray()).To(HaveLen(1))

				_ = oldStatusBitmap
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents Delete operation", func() {
		It("should delete payload and remove bitmap indexes", func() {
			key := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("to be deleted"),
									StringAttributes: map[string]string{
										"deletable": "yes",
									},
									NumericAttributes: map[string]uint64{
										"importance": 5,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify entity exists
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				_, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			// Now delete the entity
			deleteIterator := pusher.NewPushIterator()
			deleteKey := events.OPDelete(key)
			deleteBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Delete:  &deleteKey,
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				deleteIterator.Push(ctx, deleteBatch)
				deleteIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(deleteIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify entity is deleted
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				_, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).To(HaveOccurred())

				// Verify bitmap index is removed
				_, err = q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "deletable",
					Value: "yes",
				})
				Expect(err).To(HaveOccurred())

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents Expire operation", func() {
		It("should expire payload and remove bitmap indexes", func() {
			key := common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         10, // Short BTL
									Owner:       owner,
									Content:     []byte("will expire"),
									StringAttributes: map[string]string{
										"expirable": "yes",
									},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Now expire the entity
			expireIterator := pusher.NewPushIterator()
			expireKey := events.OPExpire(key)
			expireBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 111, // After expiration
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Expire:  &expireKey,
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				expireIterator.Push(ctx, expireBatch)
				expireIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(expireIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify entity is expired (deleted)
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				_, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).To(HaveOccurred())
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents ExtendBTL operation", func() {
		It("should extend expiration and update bitmap indexes", func() {
			key := common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("content"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify original expiration
			var originalExpiration uint64
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				originalExpiration = row.NumericAttributes.Values["$expiration"]
				Expect(originalExpiration).To(Equal(uint64(600))) // 100 + 500
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			// Now extend BTL
			extendIterator := pusher.NewPushIterator()
			extendBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 200,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								ExtendBTL: &events.OPExtendBTL{
									Key: key,
									BTL: 1000,
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				extendIterator.Push(ctx, extendBatch)
				extendIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(extendIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify extended expiration
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				newExpiration := row.NumericAttributes.Values["$expiration"]
				Expect(newExpiration).To(Equal(uint64(1200))) // 200 + 1000

				// Verify old expiration bitmap is removed
				oldExpBitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
					Name:  "$expiration",
					Value: 600,
				})
				Expect(err).To(HaveOccurred())

				// Verify new expiration bitmap exists
				newExpBitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
					Name:  "$expiration",
					Value: 1200,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(newExpBitmap.ToArray()).To(HaveLen(1))

				_ = oldExpBitmap
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents ChangeOwner operation", func() {
		It("should change owner and update bitmap indexes", func() {
			key := common.HexToHash("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
			originalOwner := common.HexToAddress("0x1111111111111111111111111111111111111111")
			newOwner := common.HexToAddress("0x2222222222222222222222222222222222222222")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             originalOwner,
									Content:           []byte("content"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify original owner
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.StringAttributes.Values["$owner"]).To(Equal(strings.ToLower(originalOwner.Hex())))
				Expect(row.StringAttributes.Values["$creator"]).To(Equal(strings.ToLower(originalOwner.Hex())))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			// Now change owner
			changeOwnerIterator := pusher.NewPushIterator()
			changeOwnerBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								ChangeOwner: &events.OPChangeOwner{
									Key:   key,
									Owner: newOwner,
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				changeOwnerIterator.Push(ctx, changeOwnerBatch)
				changeOwnerIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(changeOwnerIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify new owner and creator preserved
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.StringAttributes.Values["$owner"]).To(Equal(strings.ToLower(newOwner.Hex())))
				// $creator should be preserved
				Expect(row.StringAttributes.Values["$creator"]).To(Equal(strings.ToLower(originalOwner.Hex())))

				// Verify old owner bitmap is removed
				oldOwnerBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "$owner",
					Value: strings.ToLower(originalOwner.Hex()),
				})
				Expect(err).To(HaveOccurred())

				// Verify new owner bitmap exists
				newOwnerBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "$owner",
					Value: strings.ToLower(newOwner.Hex()),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(newOwnerBitmap.ToArray()).To(HaveLen(1))

				_ = oldOwnerBitmap
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents multiple updates to same key", func() {
		It("should only apply the last update in a block", func() {
			key := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("original"),
									StringAttributes: map[string]string{
										"status": "v0",
									},
									NumericAttributes: map[string]uint64{
										"version": 0,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Send a single update (the last one) - this is the only one that will be applied
			// When multiple updates to the same key exist in a block, only the last one is applied
			updateIterator := pusher.NewPushIterator()
			updateBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 1,
								OpIndex: 0,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("final update"),
									StringAttributes: map[string]string{
										"status": "v3",
									},
									NumericAttributes: map[string]uint64{
										"version": 3,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				updateIterator.Push(ctx, updateBatch)
				updateIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(updateIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify the update was applied
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Payload).To(Equal([]byte("final update")))
				Expect(row.StringAttributes.Values["status"]).To(Equal("v3"))
				Expect(row.NumericAttributes.Values["version"]).To(Equal(uint64(3)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should skip non-last updates and only process the last update for a key", func() {
			key := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0001")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("original"),
									StringAttributes: map[string]string{
										"status": "v0",
									},
									NumericAttributes: map[string]uint64{
										"version": 0,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Send multiple updates in the same block - only the LAST one should be processed
			// The code uses `continue operationLoop` to skip non-last updates and continue to next operation
			updateIterator := pusher.NewPushIterator()
			updateBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("first update - skipped"),
									StringAttributes: map[string]string{
										"status": "v1",
									},
									NumericAttributes: map[string]uint64{
										"version": 1,
									},
								},
							},
							{
								TxIndex: 0,
								OpIndex: 1,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("second update - last one"),
									StringAttributes: map[string]string{
										"status": "v2",
									},
									NumericAttributes: map[string]uint64{
										"version": 2,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				updateIterator.Push(ctx, updateBatch)
				updateIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(updateIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// With `continue operationLoop`, non-last updates are skipped but processing
			// continues to the next operation. The last update for the key is applied.
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				// The last update (second one) should be applied
				Expect(row.Payload).To(Equal([]byte("second update - last one")))
				Expect(row.StringAttributes.Values["status"]).To(Equal("v2"))
				Expect(row.NumericAttributes.Values["version"]).To(Equal(uint64(2)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents skip already processed blocks", func() {
		It("should skip blocks that have already been processed", func() {
			key := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create entity at block 100
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("original"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Try to replay the same block - should be skipped
			replayIterator := pusher.NewPushIterator()
			replayBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100, // Same block number
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("should be ignored"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				replayIterator.Push(ctx, replayBatch)
				replayIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(replayIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify original content is preserved
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Payload).To(Equal([]byte("original")))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents batch error handling", func() {
		It("should return error when batch contains an error", func() {
			// Create a custom iterator that yields an error
			errorIterator := func(yield func(arkivevents.BatchOrError) bool) {
				yield(arkivevents.BatchOrError{
					Batch: events.BlockBatch{},
					Error: errors.New("simulated batch error"),
				})
			}

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(errorIterator))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("simulated batch error"))
		})
	})

	Describe("FollowEvents system attributes", func() {
		It("should set all system attributes correctly on create", func() {
			key := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			iterator := pusher.NewPushIterator()
			batch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 5,
								OpIndex: 3,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("content"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				iterator.Push(ctx, batch)
				iterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(iterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())

				// String attributes
				Expect(row.StringAttributes.Values["$owner"]).To(Equal(strings.ToLower(owner.Hex())))
				Expect(row.StringAttributes.Values["$creator"]).To(Equal(strings.ToLower(owner.Hex())))
				Expect(row.StringAttributes.Values["$key"]).To(Equal(strings.ToLower(key.Hex())))

				// Numeric attributes
				Expect(row.NumericAttributes.Values["$expiration"]).To(Equal(uint64(600))) // 100 + 500
				Expect(row.NumericAttributes.Values["$createdAtBlock"]).To(Equal(uint64(100)))
				Expect(row.NumericAttributes.Values["$lastModifiedAtBlock"]).To(Equal(uint64(100)))
				Expect(row.NumericAttributes.Values["$txIndex"]).To(Equal(uint64(5)))
				Expect(row.NumericAttributes.Values["$opIndex"]).To(Equal(uint64(3)))

				// Verify sequence calculation
				expectedSequence := uint64(100)<<32 | uint64(5)<<16 | uint64(3)
				Expect(row.NumericAttributes.Values["$sequence"]).To(Equal(expectedSequence))

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
