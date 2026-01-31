package pusher

import (
	"context"
	"iter"

	arkivevents "github.com/salad-x-golem/arkiv-events"
	"github.com/salad-x-golem/arkiv-events/events"
)

type PushIterator struct {
	ch chan arkivevents.BatchOrError
}

func NewPushIterator() *PushIterator {
	return &PushIterator{
		ch: make(chan arkivevents.BatchOrError),
	}
}

func (i *PushIterator) Iterator() iter.Seq[arkivevents.BatchOrError] {
	return func(yield func(arkivevents.BatchOrError) bool) {
		for batch := range i.ch {
			if !yield(batch) {
				break
			}
		}
	}
}

func (i *PushIterator) Push(
	ctx context.Context,
	batch events.BlockBatch,
) {
	i.ch <- arkivevents.BatchOrError{
		Batch: batch,
		Error: nil,
	}
}

func (i *PushIterator) Close() {
	close(i.ch)
}
