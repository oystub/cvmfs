package scheduler

import (
	"fmt"
	"sync"
)

type SuccessorRelationship struct {
	Predecessor *Task2
	Successor   *Task2
	// If true, successor will be aborted if predecessor fails
	AbortOnPredecessorFailure bool
}

type ParentChildRelationship struct {
	Parent                    *Task2
	Child                     *Task2
	AbortParentOnChildFailure bool
}

type Task2 struct {
	Name     string
	StatusCv sync.Cond
	Status   TaskStatus
	Done     bool
	Started  bool

	// Parent/Child relationships for subtasks
	ChildrenCv        sync.Cond
	ChildrenRemaining int
	Parent            *ParentChildRelationship
	Children          []*ParentChildRelationship

	// Predecessor/Successor relationships
	PredecessorsCv        sync.Cond
	PredecessorsRemaining int
	Predecessors          []*SuccessorRelationship
	Successors            []*SuccessorRelationship

	ResourcesRequired []*Resource
	ResourcePool      *ResourcePool

	// Channels for communication between tasks
	abort chan interface{}
	done  chan interface{}
}

func NewTask2(name string, pool *ResourcePool) *Task2 {
	return &Task2{
		Name:     name,
		StatusCv: sync.Cond{L: &sync.Mutex{}},
		Status:   TS_CREATED,
		Done:     false,
		Started:  false,

		ChildrenCv:        sync.Cond{L: &sync.Mutex{}},
		ChildrenRemaining: 0,
		Parent:            nil,
		Children:          []*ParentChildRelationship{},

		PredecessorsCv:        sync.Cond{L: &sync.Mutex{}},
		PredecessorsRemaining: 0,
		Predecessors:          []*SuccessorRelationship{},
		Successors:            []*SuccessorRelationship{},

		ResourcePool:      pool,
		ResourcesRequired: []*Resource{},

		abort: make(chan interface{}),
		done:  make(chan interface{}),
	}
}

func (t *Task2) AddChild(child *Task2, abortParentOnChildFailure bool) {
	relationship := &ParentChildRelationship{
		Parent:                    t,
		Child:                     child,
		AbortParentOnChildFailure: abortParentOnChildFailure,
	}
	t.ChildrenCv.L.Lock()
	t.Children = append(t.Children, relationship)
	t.ChildrenRemaining++
	t.ChildrenCv.L.Unlock()

	child.ChildrenCv.L.Lock()
	child.Parent = relationship
	child.ChildrenCv.L.Unlock()
}

func (t *Task2) AddSuccessor(successor *Task2, abortOnPredecessorFailure bool) {
	relationship := &SuccessorRelationship{
		Predecessor:               t,
		Successor:                 successor,
		AbortOnPredecessorFailure: abortOnPredecessorFailure,
	}

	t.PredecessorsCv.L.Lock()
	t.Successors = append(t.Successors, relationship)
	t.PredecessorsCv.L.Unlock()

	successor.PredecessorsCv.L.Lock()
	successor.Predecessors = append(successor.Predecessors, relationship)
	successor.PredecessorsRemaining++
	successor.PredecessorsCv.L.Unlock()
}

func (t *Task2) Then(successor *Task2, abortOnPredecessorFailure bool, abortParentOnChildFailure bool) {
	t.AddSuccessor(successor, abortOnPredecessorFailure)

	// If t is the child of another task, the successor should also be a child of that task
	if t.Parent != nil {
		t.Parent.Parent.AddChild(successor, abortParentOnChildFailure)
	}
}

func (t *Task2) Start() {
	if t.Parent != nil {
		t.Parent.Parent.StatusCv.L.Lock()
		for t.Parent.Parent.Started == false {
			t.Parent.Parent.StatusCv.Wait()
		}
		t.Parent.Parent.StatusCv.L.Unlock()
	}

	t.PredecessorsCv.L.Lock()
	for t.PredecessorsRemaining > 0 {
		t.PredecessorsCv.Wait()
	}
	t.PredecessorsCv.L.Unlock()

	// Acquire all required resources
	AcquireMultiple(t.ResourcesRequired)

	t.StatusCv.L.Lock()
	t.Started = true
	t.Status = TS_RUNNING
	fmt.Printf("Task %s started\n", t.Name)
	t.StatusCv.Broadcast()
	t.StatusCv.L.Unlock()
}

func (t *Task2) Complete(status TaskStatus) {
	// Wait for children to complete
	t.ChildrenCv.L.Lock()
	if t.ChildrenRemaining > 0 {
		for t.ChildrenRemaining > 0 {
			t.ChildrenCv.Wait()
		}
	}
	t.ChildrenCv.L.Unlock()

	// Release all resources
	ReleaseMultiple(t.ResourcesRequired)

	// Update the status
	t.StatusCv.L.Lock()
	t.Status = status
	t.Done = true
	t.StatusCv.Broadcast()
	t.StatusCv.L.Unlock()

	// Notify parent that this task is done
	if t.Parent != nil {
		t.Parent.Parent.ChildrenCv.L.Lock()
		t.Parent.Parent.ChildrenRemaining--
		t.Parent.Parent.ChildrenCv.Broadcast()
		t.Parent.Parent.ChildrenCv.L.Unlock()
	}

	// Notify successors that this task is done
	t.PredecessorsCv.L.Lock()
	for _, successor := range t.Successors {
		successor.Successor.PredecessorsCv.L.Lock()
		successor.Successor.PredecessorsRemaining--
		successor.Successor.PredecessorsCv.Broadcast()
		successor.Successor.PredecessorsCv.L.Unlock()
	}
	t.PredecessorsCv.L.Unlock()

	fmt.Printf("Task %s completed with status %s\n", t.Name, status.String())

}
