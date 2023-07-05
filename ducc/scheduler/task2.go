package scheduler

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
)

type SuccessorRelationship struct {
	Predecessor *Task2
	Successor   *Task2
	// If true, successor will be aborted if predecessor fails
	AbortOnPredecessorFailure bool
}

type ParentChildRelationship struct {
	Parent                   *Task2
	Child                    *Task2
	FailParentOnChildFailure bool
}

type Task2 struct {
	Name string

	statusCv   sync.Cond // Guards status, done, started, retries, and maxRetries
	status     TaskStatus
	done       bool
	started    bool
	retries    int
	maxRetries int

	// Parent/Child relationships for subtasks
	childrenMutex     sync.Mutex // Guards childrenRemaining and children
	childrenRemaining int
	children          []*ParentChildRelationship
	childCompleted    chan *ParentChildRelationship
	parent            *ParentChildRelationship

	// Predecessor/Successor relationships
	predecessorsCv        sync.Cond // Guards predecessorsRemaining, predecessors, and successors
	predecessorsRemaining int
	predecessors          []*SuccessorRelationship
	successors            []*SuccessorRelationship

	// Resources required by this task
	// resourcesRequired can only be changed during task creation - no mutex required
	resourcesRequired []*Resource
	ResourcePool      *ResourcePool

	// Logging
	logger    *log.Logger
	logBuffer *bytes.Buffer

	// Channels for communication between tasks
	Interrupt chan TaskStatus
}

func NewTask2(name string, pool *ResourcePool, maxRetries int) *Task2 {
	buffer := bytes.NewBuffer([]byte{})

	return &Task2{
		Name:       name,
		statusCv:   sync.Cond{L: &sync.Mutex{}},
		status:     TS_CREATED,
		done:       false,
		started:    false,
		retries:    0,
		maxRetries: maxRetries,

		childrenRemaining: 0,
		parent:            nil,
		childrenMutex:     sync.Mutex{},
		children:          []*ParentChildRelationship{},
		childCompleted:    make(chan *ParentChildRelationship, 1),

		predecessorsCv:        sync.Cond{L: &sync.Mutex{}},
		predecessorsRemaining: 0,
		predecessors:          []*SuccessorRelationship{},
		successors:            []*SuccessorRelationship{},

		ResourcePool:      pool,
		resourcesRequired: []*Resource{},

		logBuffer: buffer,
		logger:    log.New(buffer, "", log.LstdFlags),

		Interrupt: make(chan TaskStatus, 1),
	}
}

func (t *Task2) AddRequiredResources(resources ...*Resource) error {
	t.statusCv.L.Lock()
	defer t.statusCv.L.Unlock()
	if t.status != TS_CREATED {
		return fmt.Errorf("cannot add required resources to task that has already started")
	}
	t.resourcesRequired = append(t.resourcesRequired, resources...)
	return nil
}

func (t *Task2) AddChild(child *Task2, failParentOnChildFailure bool) error {
	relationship := &ParentChildRelationship{
		Parent:                   t,
		Child:                    child,
		FailParentOnChildFailure: failParentOnChildFailure,
	}
	t.childrenMutex.Lock()
	t.statusCv.L.Lock()
	if t.done {
		t.statusCv.L.Unlock()
		t.childrenMutex.Unlock()
		return fmt.Errorf("cannot add child to completed task")
	}
	t.statusCv.L.Unlock()

	t.children = append(t.children, relationship)
	t.childrenRemaining++
	t.childrenMutex.Unlock()

	child.childrenMutex.Lock()
	child.parent = relationship
	child.childrenMutex.Unlock()

	return nil
}

func (t *Task2) AddSuccessor(successor *Task2, abortOnPredecessorFailure bool) error {
	relationship := &SuccessorRelationship{
		Predecessor:               t,
		Successor:                 successor,
		AbortOnPredecessorFailure: abortOnPredecessorFailure,
	}

	t.predecessorsCv.L.Lock()
	t.statusCv.L.Lock()
	if t.done {
		t.statusCv.L.Unlock()
		t.predecessorsCv.L.Unlock()
		return fmt.Errorf("cannot add successor to completed task")
	}
	t.statusCv.L.Unlock()

	t.successors = append(t.successors, relationship)
	t.predecessorsCv.L.Unlock()

	successor.predecessorsCv.L.Lock()
	successor.predecessors = append(successor.predecessors, relationship)
	successor.predecessorsRemaining++
	successor.predecessorsCv.L.Unlock()

	return nil
}

// Convenience function for adding a successor and sibling relationship at the same time
func (t *Task2) Then(successor *Task2, abortOnPredecessorFailure bool, failParentOnChildFailure bool) {
	t.AddSuccessor(successor, abortOnPredecessorFailure)
	// If t is the child of another task, the successor should also be a child of that task
	if t.parent != nil {
		t.parent.Parent.AddChild(successor, failParentOnChildFailure)
	}
}

func (t *Task2) StartWhenReady() {
	t.logger.Println("[DEBUG] StartWhenReady called")
	t.statusCv.L.Lock()
	t.status = TS_WAITING
	t.statusCv.L.Unlock()

	if t.parent != nil {
		t.parent.Parent.statusCv.L.Lock()
		for !t.parent.Parent.started {
			t.logger.Printf("[DEBUG] Waiting for parent task %s to start\n", t.parent.Parent.Name)
			t.parent.Parent.statusCv.Wait()
		}
		t.parent.Parent.statusCv.L.Unlock()
	}

	t.predecessorsCv.L.Lock()
	for t.predecessorsRemaining > 0 {
		t.logger.Printf("[DEBUG] Waiting for %d predecessors to complete\n", t.predecessorsRemaining)
		t.predecessorsCv.Wait()
	}
	t.predecessorsCv.L.Unlock()

	// Acquire all required resources
	if len(t.resourcesRequired) > 0 {
		t.logger.Printf("[DEBUG] Acquiring resources")
		AcquireMultiple(t.resourcesRequired)
		t.logger.Println("[DEBUG] Resources acquired")
	}

	t.statusCv.L.Lock()
	t.started = true
	t.status = TS_RUNNING
	t.logger.Println("[INFO] Task started")
	t.statusCv.Broadcast()
	t.statusCv.L.Unlock()
}

func (t *Task2) CompleteWhenReady(status TaskStatus) {
	t.logger.Printf("[DEBUG] CompleteWhenReady called with status %s\n", status.String())
	t.childrenMutex.Lock()
	if t.childrenRemaining > 0 {
		t.childrenMutex.Unlock()
		t.logger.Printf("[DEBUG] Waiting for %d child task(s) to complete\n", t.childrenRemaining)
	WaitForChildren:
		for {
			select {
			case relationship := <-t.childCompleted:
				t.childrenMutex.Lock()
				t.childrenRemaining--
				relationship.Child.statusCv.L.Lock()
				childStatus := relationship.Child.status
				relationship.Child.statusCv.L.Unlock()
				t.logger.Printf("[DEBUG] Child task %s completed with status %s\n", relationship.Child.Name, childStatus.String())
				// If a required child failed, this task should fail as well
				if relationship.FailParentOnChildFailure && childStatus != TS_SUCCESS {
					status = TS_FAILED
					// Abort all other child tasks
					t.logger.Printf("[ERROR] Required child task %s completed with status %s. Aborting other child tasks\n", relationship.Child.Name, childStatus.String())
					for _, child := range t.children {
						select {
						case child.Child.Interrupt <- TS_ABORTED:
						default:
						}
					}
				}
				if t.childrenRemaining == 0 {
					// Retain the children lock until the status is updated
					t.logger.Println("[DEBUG] All child tasks completed")
					break WaitForChildren
				}
				t.childrenMutex.Unlock()
			case status := <-t.Interrupt:
				t.logger.Printf("[DEBUG] Interrupted with status flag %s\n. Forwarding interrupt to all child tasks", status.String())
				// Interrupt all child tasks
				for _, child := range t.children {
					select {
					case child.Child.Interrupt <- status:
					default:
					}
				}
			}
		}
	}

	// Release all resources
	if len(t.resourcesRequired) > 0 {
		t.logger.Println("[DEBUG] Releasing resources")
		ReleaseMultiple(t.resourcesRequired)
	}
	// Update the task status
	t.statusCv.L.Lock()
	t.status = status
	t.done = true
	t.statusCv.Broadcast()
	t.statusCv.L.Unlock()
	t.childrenMutex.Unlock()

	// Notify parent that this task is done
	if t.parent != nil {
		t.parent.Parent.childCompleted <- t.parent
	}

	// Notify successors that this task is done
	t.predecessorsCv.L.Lock()
	for _, successor := range t.successors {
		successor.Successor.predecessorsCv.L.Lock()
		successor.Successor.predecessorsRemaining--
		successor.Successor.predecessorsCv.Broadcast()
		successor.Successor.predecessorsCv.L.Unlock()
	}
	t.predecessorsCv.L.Unlock()

	t.logger.Printf("[INFO] Task completed with status %s\n", status.String())
}

func (t *Task2) Retry() bool {
	t.statusCv.L.Lock()
	defer t.statusCv.L.Unlock()

	if t.done {
		t.logger.Printf("[DEBUG] Not retrying task. Task is already done\n")
		return false
	}

	if t.retries < t.maxRetries {
		t.logger.Printf("[DEBUG] Retrying task. Retry %d of %d\n", t.retries+1, t.maxRetries)
		if len(t.resourcesRequired) > 0 {
			t.childrenMutex.Lock()
			childrenRemaining := t.childrenRemaining
			t.childrenMutex.Unlock()
			if childrenRemaining > 0 {
				t.logger.Printf("[WARN] Releasing and re-acquiring resources while %d child task(s) are still remaining\n", childrenRemaining)
			}
			t.logger.Printf("[DEBUG] Releasing and re-acquiring resources")
			ReleaseMultiple(t.resourcesRequired)
			AcquireMultiple(t.resourcesRequired)
		}
		t.retries++
		return true
	}
	t.logger.Printf("[DEBUG] Not retrying task. Tried %d time(s), and max retries is %d\n", t.retries+1, t.maxRetries)
	return false
}

func VisualizeTaskGraph(task *Task2) {
	g := graph.New(taskNameHash, graph.Directed())
	addAllSuccessorsAndChildren(g, task, true)
	file, _ := os.Create("/root/my-graph.gv")
	_ = draw.DOT(g, file)

}

func addAllSuccessorsAndChildren(g graph.Graph[string, *Task2], task *Task2, root bool) {
	var bgColor string
	if task.status == TS_SUCCESS {
		bgColor = "green"
	} else if task.status == TS_FAILED {
		bgColor = "red"
	} else if task.status == TS_ABORTED {
		bgColor = "gray"
	} else {
		bgColor = "white"
	}
	g.AddVertex(task, graph.VertexAttribute("label", task.Name+"\n"+task.status.String()), graph.VertexAttribute("fillcolor", bgColor), graph.VertexAttribute("style", "filled"), graph.VertexAttribute("shape", "box"))
	for _, successor := range task.successors {
		addAllSuccessorsAndChildren(g, successor.Successor, false)
		g.AddEdge(task.Name, successor.Successor.Name, graph.EdgeAttribute("color", "black"), graph.EdgeAttribute("weight", "2"))
	}
	for _, child := range task.children {
		addAllSuccessorsAndChildren(g, child.Child, false)
		g.AddEdge(task.Name, child.Child.Name, graph.EdgeAttribute("color", "blue"), graph.EdgeAttribute("style", "dashed"), graph.EdgeAttribute("weight", "4"))
	}
}

func taskNameHash(task *Task2) string {
	return task.Name
}
