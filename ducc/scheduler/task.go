package scheduler

import (
	"context"
	"fmt"
	"sync"
)

type TaskStatus int

const (
	TS_CREATED TaskStatus = iota
	TS_WAITING
	TS_RUNNING
	TS_SUCCESS
	TS_FAILED
	TS_ABORTED
	TS_CANCELLED
)

func (ts TaskStatus) String() string {
	switch ts {
	case TS_CREATED:
		return "CREATED"
	case TS_WAITING:
		return "WAITING"
	case TS_RUNNING:
		return "RUNNING"
	case TS_SUCCESS:
		return "SUCCESS"
	case TS_FAILED:
		return "FAILED"
	case TS_ABORTED:
		return "ABORTED"
	case TS_CANCELLED:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

type OnFailureAction int

const (
	OF_ABORT = iota
	OF_CONTINUE
)

type Task struct {
	Id            int64
	Action        Action
	Done          bool
	DoneCv        sync.Cond
	Status        TaskStatus
	Prerequisites []*Task
	Parents       []*Task
	Resources     []*Resource
	SubTasks      []*SubTask
	SubTasksMutex sync.Mutex
}

// A subtask is a task that is part of a larger task.
// What happens if a subtask fails?
// - Abort the whole parent task, and all its subtasks
// - Continue the parent task, but note that this subtask failed
type SubTask struct {
	Task      *Task
	OnFailure OnFailureAction
}

func (t *Task) AddSubTask(task *Task, onFailure OnFailureAction) {
	subTask := SubTask{
		Task:      task,
		OnFailure: onFailure,
	}
	t.SubTasksMutex.Lock()
	t.SubTasks = append(t.SubTasks, &subTask)
	t.SubTasksMutex.Unlock()
}

func (t *Task) AddMultipleSubTasks(subTasks []*SubTask) {
	t.SubTasksMutex.Lock()
	t.SubTasks = append(t.SubTasks, subTasks...)
	t.SubTasksMutex.Unlock()
}

func (t *Task) GetStatus() TaskStatus {
	t.DoneCv.L.Lock()
	defer t.DoneCv.L.Unlock()
	return t.Status
}

func (t *Task) setStatus(status TaskStatus) {
	t.DoneCv.L.Lock()
	defer t.DoneCv.L.Unlock()
	t.Status = status
}
func (t *Task) setDone(done bool) {
	t.DoneCv.L.Lock()
	defer t.DoneCv.L.Unlock()
	t.Done = done
}

func NewTask(id int64, job Action, prerequisites []*Task, resources []*Resource) *Task {
	t := Task{
		Id:            id,
		Action:        job,
		Done:          false,
		Status:        TS_CREATED,
		Prerequisites: prerequisites,
		Resources:     resources,
		DoneCv:        sync.Cond{L: &sync.Mutex{}},
	}
	return &t
}

type Action interface {
	Work(ctx context.Context) error
}

func (t *Task) Run(ctx context.Context) error {
	defer t.DoneCv.Broadcast()
	defer t.setDone(true)

	// 1. Wait for prerequisites to finish
	for _, p := range t.Prerequisites {
		p.DoneCv.L.Lock()
		for !p.Done {
			p.DoneCv.Wait()
		}
		success := p.Status == TS_SUCCESS
		p.DoneCv.L.Unlock()
		if !success {
			t.setStatus(TS_ABORTED)
			return nil
		}

	}

	// 2a. Acquire any needed resource(s)
	AcquireMultiple(t.Resources)

	// 3. Perform the action
	var err error
	t.setStatus(TS_RUNNING)
	if t.Action != nil {
		err = t.Action.Work(ctx)
	}

	// 2b. Release any acquired resource(s)
	ReleaseMultiple(t.Resources)

	if err != nil {
		fmt.Printf("Task failed with error %s\n", err.Error())
		t.setStatus(TS_FAILED)
		return nil
	}

	// 4. Run subtask(s)
	t.SubTasksMutex.Lock()
	if len(t.SubTasks) > 0 {
		subTaskDoneCount := 0
		subTaskDone := make(chan *SubTask)
		ctxSubTasks, cancelSubTasks := context.WithCancel(ctx)
		for _, subTask := range t.SubTasks {
			go func(subTask *SubTask, done chan<- *SubTask) {
				subTask.Task.Run(ctxSubTasks)
				done <- subTask
			}(subTask, subTaskDone)
		}
		t.SubTasksMutex.Unlock()

		success := true
	WaitForSubTasks:
		for {
			select {
			case <-ctx.Done():
				// The parent task was stopped, we should abort
				t.setStatus(TS_ABORTED)
				success = false
				cancelSubTasks()
			case subtask := <-subTaskDone:
				if subtask.OnFailure == OF_ABORT && subtask.Task.GetStatus() != TS_SUCCESS {
					// A required subtask failed, this task will also fail
					t.setStatus(TS_FAILED)
					success = false
					cancelSubTasks()
				}
				subTaskDoneCount++
				if subTaskDoneCount == len(t.SubTasks) {
					if success {
						t.setStatus(TS_SUCCESS)
					}
					cancelSubTasks() // Clean up subtask context, for good measure
					break WaitForSubTasks
				}
			}
		}
	} else {
		// No subtasks to run
		t.SubTasksMutex.Unlock()
		t.setStatus(TS_SUCCESS)
	}

	return nil
}

type Step struct {
	Name          string
	Description   string
	Critical      bool
	State         TaskStatus
	Pool          *ResourcePool
	Prerequisites []*Step
	Resources     []*Resource
	Logs          []string
}

func (s *Step) WaitForPrerequisites() {
	// TODO
}

func (s *Step) WaitForResources() {
	AcquireMultiple(s.Resources)
}

func (s *Step) Start() {
	fmt.Printf("Waiting for prerequisites for step %s\n", s.Name)
	s.WaitForPrerequisites()
	fmt.Printf("Waiting for resources for step %s\n", s.Name)
	AcquireMultiple(s.Resources)
	fmt.Printf("Ready to start step %s\n", s.Name)
}

func (s *Step) Complete(status TaskStatus) {
	ReleaseMultiple(s.Resources)
	fmt.Printf("Step %s completed with status %s\n", s.Name, status.String())
}

func (s *Step) Then(nextStep *Step) {
	nextStep.Prerequisites = append(nextStep.Prerequisites, s)
}
