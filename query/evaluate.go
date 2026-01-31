package query

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/salad-x-golem/sqlite-bitmap-store/store"
)

func (t *AST) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	if t.Expr == nil {
		ids, err := q.EvaluateAll(ctx)
		if err != nil {
			return nil, err
		}
		bm := roaring64.New()
		bm.AddMany(ids)
		return bm, nil
	}
	return t.Expr.Evaluate(ctx, q)
}

func (e *ASTExpr) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	return e.Or.Evaluate(ctx, q)
}

func (e *ASTOr) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	var tmp *roaring64.Bitmap = nil

	for _, term := range e.Terms {
		bm, err := term.Evaluate(ctx, q)
		if err != nil {
			return nil, err
		}
		if tmp == nil {
			tmp = bm
		} else {
			tmp.Or(bm)
		}
	}

	return tmp, nil
}

func (e *ASTAnd) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	var tmp *roaring64.Bitmap = nil

	for _, term := range e.Terms {
		bm, err := term.Evaluate(ctx, q)
		if err != nil {
			return nil, err
		}
		if tmp == nil {
			tmp = bm
		} else {
			tmp.And(bm)
		}
	}

	return tmp, nil
}

func (e *ASTTerm) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	switch {
	case e.Assign != nil:
		return e.Assign.Evaluate(ctx, q)
	case e.Inclusion != nil:
		return e.Inclusion.Evaluate(ctx, q)
	case e.LessThan != nil:
		return e.LessThan.Evaluate(ctx, q)
	case e.LessOrEqualThan != nil:
		return e.LessOrEqualThan.Evaluate(ctx, q)
	case e.GreaterThan != nil:
		return e.GreaterThan.Evaluate(ctx, q)
	case e.GreaterOrEqualThan != nil:
		return e.GreaterOrEqualThan.Evaluate(ctx, q)
	case e.Glob != nil:
		return e.Glob.Evaluate(ctx, q)
	default:
		return nil, fmt.Errorf("unknown equal expression: %v", e)
	}
}

func (e *Glob) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	bm := roaring64.New()

	var bitmaps []*store.Bitmap

	if e.IsNot {
		bitmaps, err = q.EvaluateStringAttributeValueNotGlob(ctx, store.EvaluateStringAttributeValueNotGlobParams{
			Name:  e.Var,
			Value: e.Value,
		})
		if err != nil {
			return nil, err
		}
	} else {
		bitmaps, err = q.EvaluateStringAttributeValueGlob(ctx, store.EvaluateStringAttributeValueGlobParams{
			Name:  e.Var,
			Value: e.Value,
		})
		if err != nil {
			return nil, err
		}
	}

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

func (e *LessThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueLowerThan(ctx, store.EvaluateStringAttributeValueLowerThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}
	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueLowerThan(ctx, store.EvaluateNumericAttributeValueLowerThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

func (e *LessOrEqualThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueLessOrEqualThan(ctx, store.EvaluateStringAttributeValueLessOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}
	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueLessOrEqualThan(ctx, store.EvaluateNumericAttributeValueLessOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

func (e *GreaterThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueGreaterThan(ctx, store.EvaluateStringAttributeValueGreaterThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}

	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueGreaterThan(ctx, store.EvaluateNumericAttributeValueGreaterThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

func (e *GreaterOrEqualThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueGreaterOrEqualThan(ctx, store.EvaluateStringAttributeValueGreaterOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}

	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueGreaterOrEqualThan(ctx, store.EvaluateNumericAttributeValueGreaterOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

func (e *Equality) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	if e.Value.String != nil {

		if e.IsNot {

			var bitmaps []*store.Bitmap
			bitmaps, err = q.EvaluateStringAttributeValueNotEqual(ctx, store.EvaluateStringAttributeValueNotEqualParams{
				Name:  e.Var,
				Value: *e.Value.String,
			})
			if err != nil {
				return nil, err
			}

			bm := roaring64.New()

			for _, bitmap := range bitmaps {
				bm.Or(bitmap.Bitmap)
			}

			return bm, nil

		} else {
			bm, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
				Name:  e.Var,
				Value: *e.Value.String,
			})

			if err == sql.ErrNoRows {
				return roaring64.New(), nil
			}

			if err != nil {
				return nil, err
			}

			return bm.Bitmap, nil
		}
	} else {
		if e.IsNot {

			var bitmaps []*store.Bitmap
			bitmaps, err = q.EvaluateNumericAttributeValueNotEqual(ctx, store.EvaluateNumericAttributeValueNotEqualParams{
				Name:  e.Var,
				Value: *e.Value.Number,
			})
			if err != nil {
				return nil, err
			}

			bm := roaring64.New()
			for _, bitmap := range bitmaps {
				bm.Or(bitmap.Bitmap)
			}

			return bm, nil
		} else {
			bitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
				Name:  e.Var,
				Value: *e.Value.Number,
			})

			if err == sql.ErrNoRows {
				return roaring64.New(), nil
			}

			if err != nil {
				return nil, err
			}

			return bitmap.Bitmap, nil
		}
	}

}

func (e *Inclusion) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	if len(e.Values.Strings) != 0 {

		var bitmaps []*store.Bitmap

		if e.IsNot {
			bitmaps, err = q.EvaluateStringAttributeValueNotInclusion(ctx, store.EvaluateStringAttributeValueNotInclusionParams{
				Name:   e.Var,
				Values: e.Values.Strings,
			})
			if err != nil {
				return nil, err
			}
		} else {

			bitmaps, err = q.EvaluateStringAttributeValueInclusion(ctx, store.EvaluateStringAttributeValueInclusionParams{
				Name:   e.Var,
				Values: e.Values.Strings,
			})
			if err != nil {
				return nil, err
			}
		}
		bm := roaring64.New()
		for _, bitmap := range bitmaps {
			bm.Or(bitmap.Bitmap)
		}
		return bm, nil

	} else {
		var bitmaps []*store.Bitmap

		if e.IsNot {
			bitmaps, err = q.EvaluateNumericAttributeValueNotInclusion(ctx, store.EvaluateNumericAttributeValueNotInclusionParams{
				Name:   e.Var,
				Values: e.Values.Numbers,
			})
			if err != nil {
				return nil, err
			}
		} else {
			bitmaps, err = q.EvaluateNumericAttributeValueInclusion(ctx, store.EvaluateNumericAttributeValueInclusionParams{
				Name:   e.Var,
				Values: e.Values.Numbers,
			})
			if err != nil {
				return nil, err
			}
		}
		bm := roaring64.New()
		for _, bitmap := range bitmaps {
			bm.Or(bitmap.Bitmap)
		}
		return bm, nil
	}

}
