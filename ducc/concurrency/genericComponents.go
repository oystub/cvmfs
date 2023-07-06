package concurrency

import (
	"sort"
	"sync"

	"github.com/google/uuid"
)

type Sync[V any] struct {
	In  []RefCountedChan[InformationPacket[V]]
	Out []RefCountedChan[InformationPacket[V]]
}

func NewSync[V any](numPorts int) Sync[V] {
	out := make([]RefCountedChan[InformationPacket[V]], numPorts)
	in := make([]RefCountedChan[InformationPacket[V]], numPorts)
	for i := 0; i < numPorts; i++ {
		out[i] = NewRefCountedChan[InformationPacket[V]]()
		in[i] = NewRefCountedChan[InformationPacket[V]]()
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
	valuesByTag := make(map[uuid.UUID][]InformationPacket[V])
	mux := &sync.Mutex{}

	var wg sync.WaitGroup

	// Start a goroutine to read from each input channel.
	for i, inCh := range s.In {
		wg.Add(1)
		go func(i int, inCh RefCountedChan[InformationPacket[V]]) {
			defer wg.Done()
			for v := range inCh.ch {
				mux.Lock()
				// Is this the first value received for this tag?
				// If so, the id should not be in the map yet.
				if valuesByTag[v.Handle.id] == nil {
					valuesByTag[v.Handle.id] = make([]InformationPacket[V], 0, len(s.In))
				}

				// Append the received value to the slice for its tag.
				tag := v.Handle.id
				valuesByTag[tag] = append(valuesByTag[tag], v)

				// Check if we have received a value from all input channels for this tag.
				if len(valuesByTag[tag]) == len(s.In) {
					// If we have, send all the values to the corresponding output channel.
					wg2 := sync.WaitGroup{}
					for j, val := range valuesByTag[tag] {
						wg2.Add(1)
						go func(val InformationPacket[V], chNr int) {
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
	In  RefCountedChan[InformationPacket[V]]
	Out []RefCountedChan[InformationPacket[V]]
}

func NewBroadcast[V any](numOutputs int) Broadcast[V] {
	out := make([]RefCountedChan[InformationPacket[V]], numOutputs)
	for i := 0; i < numOutputs; i++ {
		out[i] = NewRefCountedChan[InformationPacket[V]]()
	}

	return Broadcast[V]{
		In:  NewRefCountedChan[InformationPacket[V]](),
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
	In  RefCountedChan[InformationPacket[V]]
	Out RefCountedChan[InformationPacket[[]V]]
}

func NewGather[V any]() Gather[V] {
	return Gather[V]{
		In:  NewRefCountedChan[InformationPacket[V]](),
		Out: NewRefCountedChan[InformationPacket[[]V]](),
	}
}

func (g Gather[V]) Process() {
	defer g.Out.Close()
	// The id of the parent handle is referred to as the "tag".

	// We spawn a goroutine for each individual tag.
	// Their input channels are stored in a map, with the tag as the key.
	chansByTag := make(map[uuid.UUID]chan InformationPacket[V])
	chansByTagMutex := &sync.Mutex{}

	workers := sync.WaitGroup{}

	for v := range g.In.ch {
		if v.Handle.parent == nil {
			v.Handle.Log.GetLogger().Fatal("[FATAL] Orphan handle received by Gather. This should not happen. Something is wrong with the graph.")
		}
		chansByTagMutex.Lock()
		// Is this the first value received for this tag?
		// If so, the tag should not be in the map yet.
		if chansByTag[v.Handle.parent.id] == nil {
			chansByTag[v.Handle.parent.id] = make(chan InformationPacket[V])

			// Goroutine for closing the channel and removing it from the map when the tag is completed.
			workers.Add(1)
			go func(tag uuid.UUID, wg *sync.WaitGroup) {
				defer workers.Done()
				wg.Wait()
				close(chansByTag[tag])
				delete(chansByTag, tag)
			}(v.Handle.parent.id, v.Handle.siblingsRemaining)

			// Goroutine for collecting and sending gathered values for this tag.
			workers.Add(1)
			go func(tag uuid.UUID, in <-chan InformationPacket[V], out chan<- InformationPacket[[]V]) {
				defer workers.Done()
				receivedPackets := make([]InformationPacket[V], 0)
				for v := range chansByTag[tag] {
					receivedPackets = append(receivedPackets, v)
					v.Handle.siblingsRemaining.Done()
				}
				// Sort the values by sequence number
				sort.Slice(receivedPackets, func(i, j int) bool {
					return receivedPackets[i].Handle.seqNum < receivedPackets[j].Handle.seqNum
				})
				// Gather the values and send them to the output, using the parent header.
				outputValues := make([]V, len(receivedPackets))
				for i, val := range receivedPackets {
					outputValues[i] = val.Value
				}
				out <- InformationPacket[[]V]{
					Handle: receivedPackets[0].Handle.parent.parent,
					Value:  outputValues,
				}
			}(v.Handle.parent.id, chansByTag[v.Handle.parent.id], g.Out.ch)
		}
		chansByTag[v.Handle.parent.id] <- v
		chansByTagMutex.Unlock()
	}
	workers.Wait()
}

type Sink[V any] struct {
	In RefCountedChan[InformationPacket[V]]
}

func NewSink[V any]() Sink[V] {
	return Sink[V]{
		In: NewRefCountedChan[InformationPacket[V]](),
	}
}

func (s Sink[V]) Process() {
	for range s.In.ch {
	}
}
