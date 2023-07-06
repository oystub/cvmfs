package concurrency

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type InformationPacket[V any] struct {
	Handle *PacketHandle
	Value  V
}

func NewInformationPacket[V any](value V, ctx context.Context, name string) InformationPacket[V] {
	handle := NewPacketHandle(ctx, name)
	return InformationPacket[V]{
		Handle: &handle,
		Value:  value,
	}
}

type PacketHandle struct {
	ctx  context.Context
	id   uuid.UUID
	name string
	// Relations to other headers
	parent        *PacketHandle
	children      []*PacketHandle
	childrenMutex *sync.Mutex
	previous      *PacketHandle
	next          *PacketHandle
	// Used for scatter/gather patterns
	siblingsRemaining *sync.WaitGroup
	seqId             string
	seqNum            int64
	// Logging
	Log LoggerWithBuffer
}

func NewPacketHandle(ctx context.Context, name string) PacketHandle {
	logger := NewLoggerWithBuffer()
	return PacketHandle{
		ctx:               ctx,
		id:                uuid.New(),
		name:              name,
		parent:            nil,
		children:          make([]*PacketHandle, 0),
		childrenMutex:     &sync.Mutex{},
		next:              nil,
		previous:          nil,
		siblingsRemaining: &sync.WaitGroup{},
		seqId:             "",
		seqNum:            0,
		Log:               logger,
	}
}

func (h *PacketHandle) NewChildHandle(name string) *PacketHandle {
	h.childrenMutex.Lock()
	defer h.childrenMutex.Unlock()
	logger := NewLoggerWithBuffer()
	child := PacketHandle{
		ctx:               h.ctx,
		id:                uuid.New(),
		name:              name,
		parent:            h,
		children:          make([]*PacketHandle, 0),
		childrenMutex:     &sync.Mutex{},
		previous:          nil,
		next:              nil,
		siblingsRemaining: &sync.WaitGroup{},
		seqId:             "",
		seqNum:            0,
		Log:               logger,
	}
	h.children = append(h.children, &child)
	return &child
}

type RefCountedChan[V any] struct {
	ch   chan V
	wg   *sync.WaitGroup
	once *sync.Once
	id   int64
}

func NewRefCountedChan[V any]() RefCountedChan[V] {
	return RefCountedChan[V]{
		ch:   make(chan V),
		wg:   &sync.WaitGroup{},
		once: &sync.Once{},
	}
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

func ConnectWithTypeAssert[T1 any, T2 any](in RefCountedChan[InformationPacket[T1]], out RefCountedChan[InformationPacket[T2]]) {
	out.wg.Add(1)
	go func() {
		for v := range in.ch {
			// Assert the concrete type of v.Value and use it to create a new TaggedValueWithCtx[T2]
			out.ch <- InformationPacket[T2]{
				Handle: v.Handle,
				Value:  any(v.Value).(T2), // type assert the Value field separately
			}
		}
		out.wg.Done()
		// Close the channel when all inputs have been drained.
		out.once.Do(func() {
			go out.closeWhenDone()
		})
	}()
}
