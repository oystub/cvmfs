package concurrency

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

var Idcounter int64 = 0

type Tag struct {
	id     uuid.UUID
	seqNum int
	count  int
}

type RefCountedChan[V any] struct {
	ch   chan V
	wg   *sync.WaitGroup
	once *sync.Once
	id   int64
}

func NewRefCountedChan[V any]() RefCountedChan[V] {
	id := atomic.AddInt64(&Idcounter, 1)
	return RefCountedChan[V]{
		ch:   make(chan V),
		wg:   &sync.WaitGroup{},
		once: &sync.Once{},
		id:   id,
	}
}

func Connect[T any](in RefCountedChan[T], out RefCountedChan[T]) {
	out.wg.Add(1)
	go func() {
		for v := range in.ch {
			out.ch <- v
		}
		out.wg.Done()
		// Close the channel when all inputs have been drained.
		out.once.Do(func() {
			go out.closeWhenDone()
		})
	}()
}

func ConnectWithTypeAssert[T1 any, T2 any](in RefCountedChan[TaggedValueWithCtx[T1]], out RefCountedChan[TaggedValueWithCtx[T2]]) {
	out.wg.Add(1)
	go func() {
		for v := range in.ch {
			// Assert the concrete type of v.Value and use it to create a new TaggedValueWithCtx[T2]
			out.ch <- TaggedValueWithCtx[T2]{
				TagStack: v.TagStack,
				Ctx:      v.Ctx,
				Value:    any(v.Value).(T2), // type assert the Value field separately
			}
		}
		out.wg.Done()
		// Close the channel when all inputs have been drained.
		out.once.Do(func() {
			go out.closeWhenDone()
		})
	}()
}

func (rc *RefCountedChan[V]) Close() {
	close(rc.ch)
}

func (rc *RefCountedChan[T]) closeWhenDone() {
	rc.wg.Wait()
	close(rc.ch)
}

func (rc *RefCountedChan[V]) Chan() chan V {
	return rc.ch
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
				// Is this the first value received for this tag?
				// If so, the id should not be in the map yet.
				if valuesByTag[v.TagStack[0].id] == nil {
					valuesByTag[v.TagStack[0].id] = make([]TaggedValueWithCtx[V], 0, len(s.In))
				}

				// Append the received value to the slice for its tag.
				tag := v.TagStack[0].id
				valuesByTag[tag] = append(valuesByTag[tag], v)

				// Check if we have received a value from all input channels for this tag.
				if len(valuesByTag[tag]) == len(s.In) {
					// If we have, send all the values to the corresponding output channel.
					wg2 := sync.WaitGroup{}
					for j, val := range valuesByTag[tag] {
						wg2.Add(1)
						go func(val TaggedValueWithCtx[V], chNr int) {
							s.Out[chNr].ch <- val
							wg2.Done()
						}(val, j)
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
	out := make([]RefCountedChan[TaggedValueWithCtx[V]], numOutputs)
	for i := 0; i < numOutputs; i++ {
		out[i] = NewRefCountedChan[TaggedValueWithCtx[V]]()
	}

	return Broadcast[V]{
		In:  NewRefCountedChan[TaggedValueWithCtx[V]](),
		Out: out,
	}
}

func (b Broadcast[V]) Process() {
	for input := range b.In.ch {
		for _, output := range b.Out {
			output.ch <- input
		}
	}
	for _, output := range b.Out {
		output.Close()
	}
}

type Gather[V any] struct {
	In  RefCountedChan[TaggedValueWithCtx[V]]
	Out RefCountedChan[TaggedValueWithCtx[[]V]]
}

func NewGather[V any]() Gather[V] {
	return Gather[V]{
		In:  NewRefCountedChan[TaggedValueWithCtx[V]](),
		Out: NewRefCountedChan[TaggedValueWithCtx[[]V]](),
	}
}

func (g Gather[V]) Process() {
	defer g.Out.Close()

	// Use a map to keep track of values received for each tag.
	valuesByTag := make(map[uuid.UUID][]TaggedValueWithCtx[V])
	mux := &sync.Mutex{}

	// Start a goroutine to read from each input channel.
	for v := range g.In.ch {
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

			g.Out.ch <- TaggedValueWithCtx[[]V]{
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
	In RefCountedChan[TaggedValueWithCtx[V]]
}

func NewSink[V any]() Sink[V] {
	return Sink[V]{
		In: NewRefCountedChan[TaggedValueWithCtx[V]](),
	}
}

func (s Sink[V]) Process() {
}
