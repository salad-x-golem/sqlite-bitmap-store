package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"

	"github.com/salad-x-golem/sqlite-bitmap-store/store"
	"golang.org/x/sync/errgroup"
)

type nameValue[T any] struct {
	name  string
	value T
}

type bitmapCache struct {
	st store.Querier

	stringBitmaps  map[nameValue[string]]*store.Bitmap
	numericBitmaps map[nameValue[uint64]]*store.Bitmap
}

func newBitmapCache(st store.Querier) *bitmapCache {
	return &bitmapCache{
		st:             st,
		stringBitmaps:  make(map[nameValue[string]]*store.Bitmap),
		numericBitmaps: make(map[nameValue[uint64]]*store.Bitmap),
	}
}

func (c *bitmapCache) AddToStringBitmap(ctx context.Context, name string, value string, id uint64) (err error) {
	k := nameValue[string]{name: name, value: value}
	bitmap, ok := c.stringBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetStringAttributeValueBitmap(ctx, store.GetStringAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.stringBitmaps[k] = bitmap
	}

	bitmap.Add(id)

	return nil

}

func (c *bitmapCache) RemoveFromStringBitmap(ctx context.Context, name string, value string, id uint64) (err error) {
	k := nameValue[string]{name: name, value: value}
	bitmap, ok := c.stringBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetStringAttributeValueBitmap(ctx, store.GetStringAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.stringBitmaps[k] = bitmap
	}

	bitmap.Remove(id)

	return nil

}

func (c *bitmapCache) AddToNumericBitmap(ctx context.Context, name string, value uint64, id uint64) (err error) {
	k := nameValue[uint64]{name: name, value: value}
	bitmap, ok := c.numericBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetNumericAttributeValueBitmap(ctx, store.GetNumericAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get numeric attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.numericBitmaps[k] = bitmap
	}

	bitmap.Add(id)

	return nil

}

func (c *bitmapCache) RemoveFromNumericBitmap(ctx context.Context, name string, value uint64, id uint64) (err error) {
	k := nameValue[uint64]{name: name, value: value}
	bitmap, ok := c.numericBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetNumericAttributeValueBitmap(ctx, store.GetNumericAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get numeric attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.numericBitmaps[k] = bitmap
	}

	bitmap.Remove(id)

	return nil

}

func (c *bitmapCache) Flush(ctx context.Context) (err error) {

	eg := &errgroup.Group{}

	eg.SetLimit(runtime.NumCPU())

	for _, bitmap := range c.stringBitmaps {
		if bitmap.IsEmpty() {
			continue
		}
		eg.Go(func() error {
			bitmap.RunOptimize()
			return nil
		})
	}

	for _, bitmap := range c.numericBitmaps {
		if bitmap.IsEmpty() {
			continue
		}
		eg.Go(func() error {
			bitmap.RunOptimize()
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("failed to run optimize: %w", err)
	}

	for k, bitmap := range c.stringBitmaps {

		if bitmap.IsEmpty() {
			err = c.st.DeleteStringAttributeValueBitmap(ctx, store.DeleteStringAttributeValueBitmapParams{Name: k.name, Value: k.value})
			if err != nil {
				return fmt.Errorf("failed to delete string attribute %q value %q bitmap: %w", k.name, k.value, err)
			}
			continue
		}

		err = c.st.UpsertStringAttributeValueBitmap(ctx, store.UpsertStringAttributeValueBitmapParams{Name: k.name, Value: k.value, Bitmap: bitmap})
		if err != nil {
			return fmt.Errorf("failed to upsert string attribute %q value %q bitmap: %w", k.name, k.value, err)
		}
	}

	for k, bitmap := range c.numericBitmaps {

		if bitmap.IsEmpty() {
			err = c.st.DeleteNumericAttributeValueBitmap(ctx, store.DeleteNumericAttributeValueBitmapParams{Name: k.name, Value: k.value})
			if err != nil {
				return fmt.Errorf("failed to delete numeric attribute %q value %q bitmap: %w", k.name, k.value, err)
			}
			continue
		}

		err = c.st.UpsertNumericAttributeValueBitmap(ctx, store.UpsertNumericAttributeValueBitmapParams{Name: k.name, Value: k.value, Bitmap: bitmap})
		if err != nil {
			return fmt.Errorf("failed to upsert numeric attribute %q value %q bitmap: %w", k.name, k.value, err)
		}
	}
	return nil
}
