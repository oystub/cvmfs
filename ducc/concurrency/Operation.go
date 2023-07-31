package concurrency

import (
	"context"
	"sync"

	"github.com/cvmfs/ducc/lib"
)

var localOperationsMutex sync.Mutex
var localOperations map[lib.ObjectId]GenericOperation

func initOperations() {
	localOperations = map[lib.ObjectId]GenericOperation{}
}

type InputOutputOperation[I any, O any] struct {
	Ctx       context.Context
	CtxCancel context.CancelFunc
	Status    *StatusHandle
	Input     I
	statusCv  sync.Cond
	Output    O
	Err       error
}

func (op *InputOutputOperation[I, O]) SetError(err error) {
	op.statusCv.L.Lock()
	defer op.statusCv.L.Unlock()
	op.Err = err
}

func (op *InputOutputOperation[I, O]) StatusHandle() *StatusHandle {
	return op.Status
}

func (op *InputOutputOperation[I, O]) Cancel() {
	op.Status.SetStatus(TS_Aborted)
	op.CtxCancel()
}

// Create a channel that will be closed when the operation is done.
// If called on a completed operation, the channel will be closed immediately.
// No output is sent on the channel, so it is only useful for synchronization.
func (op *InputOutputOperation[I, O]) Done() <-chan any {
	out := make(chan any)
	go func() {
		op.statusCv.L.Lock()
		// Return if status is a non-definite state
		for op.Status.Status == TS_NotStarted || op.Status.Status == TS_Running {
			op.statusCv.Wait()
		}
		close(out)
		op.statusCv.L.Unlock()
	}()
	return out
}

type GenericOperation interface {
	StatusHandle() *StatusHandle
	Cancel()
	Done() <-chan any
}

type ParentOperation struct {
	Ctx       context.Context
	CtxCancel context.CancelFunc
	Status    *StatusHandle
	Children  []GenericOperation
}

func (op ParentOperation) StatusHandle() *StatusHandle {
	return op.Status
}

func (op ParentOperation) Cancel() {
	op.Status.SetStatus(TS_Aborted)
	op.CtxCancel()
	for _, child := range op.Children {
		child.Cancel()
	}
}

func (op ParentOperation) Done() <-chan any {
	//TODO
	return make(<-chan any)
}
