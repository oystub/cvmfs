package concurrency

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

type Tag struct {
	id     uuid.UUID
	seqNum int
	count  int
}

type RefCountedChan[V any] struct {
	ch   chan V
	wg   *sync.WaitGroup
	once *sync.Once
}

func NewRefCountedChan[V any]() RefCountedChan[V] {
	return RefCountedChan[V]{
		ch: make(chan V),
		wg: &sync.WaitGroup{},
	}
}

func (rc *RefCountedChan[T]) Connect(in <-chan T, label string) {
	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done() // Decrement the WaitGroup counter when the goroutine completes.
		for v := range in {
			rc.ch <- v
		}
		// Close the channel when all inputs have been drained.
		rc.once.Do(func() {
			go rc.closeWhenDone()
		})
	}()
}

func (rc *RefCountedChan[V]) Close() {
	rc.once.Do(func() {
		close(rc.ch)
	})
}

func (rc *RefCountedChan[T]) closeWhenDone() {
	rc.wg.Wait() // Block until all the connected goroutines have completed.
	close(rc.ch)
}

type TaggedValueWithCtx[V any] struct {
	TagStack []Tag
	Ctx      context.Context
	Value    V
}

func NewTaggedValueWithCtx[V any](value V, ctx context.Context) TaggedValueWithCtx[V] {
	return TaggedValueWithCtx[V]{
		TagStack: []Tag{{id: uuid.New()}},
		Ctx:      ctx,
		Value:    value,
	}
}

type Sync[V any] struct {
	In  []RefCountedChan[TaggedValueWithCtx[V]]
	Out []RefCountedChan[TaggedValueWithCtx[V]]
}

func NewSync[V any](numPorts int) Sync[V] {
	out := make([]RefCountedChan[TaggedValueWithCtx[V]], numPorts)
	in := make([]RefCountedChan[TaggedValueWithCtx[V]], numPorts)
	for i := 0; i < numPorts; i++ {
		out[i] = NewRefCountedChan[TaggedValueWithCtx[V]]()
		in[i] = NewRefCountedChan[TaggedValueWithCtx[V]]()
	}
	return Sync[V]{
		In:  in,
		Out: out,
	}
}

// The sync should read from all inputs. When messages with the same tag are received from all inputs, they are sent to the corresponding output.
func (s Sync[V]) Process() {
	// Use a map to keep track of values received for each tag.
	// The map keys are the tags and the values are slices of TaggedValueWithCtx.
	valuesByTag := make(map[uuid.UUID][]TaggedValueWithCtx[V])
	mux := &sync.Mutex{}

	var wg sync.WaitGroup

	// Start a goroutine to read from each input channel.
	for i, inCh := range s.In {
		wg.Add(1)
		go func(i int, inCh RefCountedChan[TaggedValueWithCtx[V]]) {
			defer wg.Done()
			for v := range inCh.ch {
				mux.Lock()
				// Append the received value to the slice for its tag.
				tag := v.TagStack[0].id
				valuesByTag[tag] = append(valuesByTag[tag], v)

				// Check if we have received a value from all input channels for this tag.
				if len(valuesByTag[tag]) == len(s.In) {
					// If we have, send all the values to the corresponding output channel.
					wg2 := sync.WaitGroup{}
					for i, val := range valuesByTag[tag] {
						wg2.Add(1)
						go func(val TaggedValueWithCtx[V], i int) {
							s.Out[i].ch <- val
							wg2.Done()
						}(val, i)
					}
					wg2.Wait()
					delete(valuesByTag, tag)
				}
				mux.Unlock()
			}
		}(i, inCh)
	}
	wg.Wait()
	for _, outCh := range s.Out {
		outCh.Close()
	}
}

type Broadcast[V any] struct {
	In  RefCountedChan[TaggedValueWithCtx[V]]
	Out []RefCountedChan[TaggedValueWithCtx[V]]
}

func NewBroadcast[V any](numOutputs int) Broadcast[V] {
	out := make([]chan TaggedValueWithCtx[V], numOutputs)
	for i := 0; i < numOutputs; i++ {
		out[i] = make(chan TaggedValueWithCtx[V])
	}

	return Broadcast[V]{
		In:  make(chan TaggedValueWithCtx[V]),
		Out: out,
	}
}

func (b Broadcast[V]) Process() {
	for input := range b.In {
		for _, output := range b.Out {
			output <- input
		}
	}
	for _, output := range b.Out {
		close(output)
	}
}

type Gather[V any] struct {
	In  chan TaggedValueWithCtx[V]
	Out chan TaggedValueWithCtx[[]V]
}

func NewGather[V any]() Gather[V] {
	return Gather[V]{
		In:  make(chan TaggedValueWithCtx[V]),
		Out: make(chan TaggedValueWithCtx[[]V]),
	}
}

func (g Gather[V]) Process() {
	// Use a map to keep track of values received for each tag.
	// The map keys are the tags and the values are slices of TaggedValueWithCtx.
	defer close(g.Out)

	valuesByTag := make(map[uuid.UUID][]TaggedValueWithCtx[V])
	mux := &sync.Mutex{}

	// Start a goroutine to read from each input channel.

	for v := range g.In {
		mux.Lock()

		// Append the received value to the slice for its tag.
		tag := v.TagStack[0].id
		valuesByTag[tag] = append(valuesByTag[tag], v)

		// Check if we have received all values for this tag.
		if len(valuesByTag[tag]) == v.TagStack[0].count {
			// Sort the values by sequence number
			sort.Slice(valuesByTag[tag], func(i, j int) bool {
				return valuesByTag[tag][i].TagStack[0].seqNum < valuesByTag[tag][j].TagStack[0].seqNum
			})
			// Create a new TaggedValueWithCtx which contains all gathered values and send it to the output.
			// First of all, copy the tackstack from the first value, but remove the first tag.
			// This is because the first tag is the one that was used to identify the values that should be gathered.
			// We don't want to keep it in the output.
			newTagStack := make([]Tag, len(valuesByTag[tag][0].TagStack)-1)
			copy(newTagStack, valuesByTag[tag][0].TagStack[1:])

			outputValues := make([]V, len(valuesByTag[tag]))
			for i, val := range valuesByTag[tag] {
				outputValues[i] = val.Value
			}

			g.Out <- TaggedValueWithCtx[[]V]{
				TagStack: newTagStack,
				Ctx:      valuesByTag[tag][0].Ctx,
				Value:    outputValues,
			}
			delete(valuesByTag, tag)
		}

		mux.Unlock()
	}
}

type Sink[V any] struct {
	In    chan TaggedValueWithCtx[V]
	Label string
}

func NewSink[V any]() Sink[V] {
	return Sink[V]{
		In: make(chan TaggedValueWithCtx[V]),
	}
}

func (s Sink[V]) Process() {
	for range s.In {
		if s.Label != "" {
			fmt.Printf("Sink %s received value\n", s.Label)
		}
	}
}

func Connect[T any](in <-chan TaggedValueWithCtx[T], out chan<- TaggedValueWithCtx[T], label string) {
	go func() {
		for v := range in {
			fmt.Printf("Connecting: %s\n", label)
			out <- v
		}
		close(out)
	}()
}

func ConnectWithTypeAssert[T1 any, T2 any](in <-chan TaggedValueWithCtx[T1], out chan<- TaggedValueWithCtx[T2]) {
	go func() {
		for v := range in {

			out <- TaggedValueWithCtx[T2]{
				TagStack: v.TagStack,
				Ctx:      v.Ctx,
				Value:    any(v.Value).(T2),
			}
		}
		close(out)
	}()
}
