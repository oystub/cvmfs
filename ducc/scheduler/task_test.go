package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Create and run a single task

type BasicAction struct{}

func (a BasicAction) Work(ctx context.Context) error {
	return nil
}

type BasicTimeoutAction struct {
	timeout time.Duration
}

func (a BasicTimeoutAction) Work(ctx context.Context) error {
	select {
	case <-ctx.Done():
		fmt.Println("Task cancelled")
		return ctx.Err()
	case <-time.After(a.timeout):
		return nil
	}
}

type BasicFailAction struct{}

func (a BasicFailAction) Work(ctx context.Context) error {
	return fmt.Errorf("BasicFailAction failed")
}

func TestCreateAndRunSingleTask(t *testing.T) {
	timeout := 10 * time.Millisecond
	done := make(chan interface{})

	task := NewTask(1, BasicAction{}, []*Task{}, []*Resource{})
	go func(chan<- interface{}) {
		task.Run(context.Background())
		done <- nil
	}(done)

	select {
	case <-done:
		if task.GetStatus() != TS_SUCCESS {
			t.Errorf("Task did not finish successfully")
		}
		break
	case <-time.After(timeout):
		t.Errorf("Task did not finish in time")
		return
	}
}

func TestSubTask(t *testing.T) {
	t.Run("NestedSubtasks", func(t *testing.T) {
		nestedTasks := 100
		timeout := 10 * time.Millisecond

		rootTask := NewTask(0, BasicAction{}, []*Task{}, []*Resource{})

		rootTaskSuccess := make(chan interface{})
		go func(cv *sync.Cond) {
			cv.L.Lock()
			for rootTask.Status != TS_SUCCESS {
				cv.Wait()
			}
			cv.L.Unlock()
			rootTaskSuccess <- true
		}(&rootTask.DoneCv)

		var task *Task = rootTask
		for i := 0; i < nestedTasks; i++ {
			subTask := NewTask(2, BasicAction{}, []*Task{}, []*Resource{})
			task.AddSubTask(subTask, OF_ABORT)
			task = subTask
		}

		go rootTask.Run(context.Background())

		for {
			select {
			case <-rootTaskSuccess:
				for _, subTask := range rootTask.SubTasks {
					if subTask.Task.Status != TS_SUCCESS {
						t.Error("Root task finished without all subtasks finishing")
					}
				}
				return
			case <-time.After(timeout):
				t.Errorf("Task did not finish in time")
				return
			}

		}

	})

	t.Run("SiblingSubtasks", func(t *testing.T) {
		siblingTasks := 1

		timeout := 10 * time.Millisecond
		done := make(chan interface{})

		rootTask := NewTask(0, BasicAction{}, []*Task{}, []*Resource{})
		for i := 0; i < siblingTasks; i++ {
			subTask := NewTask(2, BasicAction{}, []*Task{}, []*Resource{})
			rootTask.AddSubTask(subTask, OF_ABORT)
		}

		go func(chan<- interface{}) {
			rootTask.Run(context.Background())
			done <- true
		}(done)

		for {
			select {
			case <-done:
				for _, subTask := range rootTask.SubTasks {
					if subTask.Task.GetStatus() != TS_SUCCESS {
						t.Error("Root task finished without all subtasks finishing")
						return
					}
				}
				return
			case <-time.After(timeout):
				t.Errorf("Task did not finish in time")
				return
			}
		}
	})

	// If a subtask with OF_ABORT fails, the parent task should also fail
	t.Run("TestParentAbort", func(t *testing.T) {
		task := NewTask(0, BasicAction{}, []*Task{}, []*Resource{})

		subTask := NewTask(1, BasicFailAction{}, []*Task{}, []*Resource{})
		task.AddSubTask(subTask, OF_ABORT)

		// Run the parent task
		done := make(chan interface{})
		go func(chan<- interface{}) {
			task.Run(context.Background())
			done <- true
		}(done)

		select {
		case <-done:
			if task.GetStatus() != TS_FAILED {
				fmt.Printf("Parent task status: %d\n", task.GetStatus())
				t.Error("Parent task did not fail after subtask failed")
			}
			return
		case <-time.After(10 * time.Millisecond):
			t.Errorf("Task did not finish in time")
			return
		}
	})

	// If a subtask with OF_CONTINUE fails, the parent task should not automatically fail
	t.Run("TestParentNoAbort", func(t *testing.T) {
		task := NewTask(0, BasicAction{}, []*Task{}, []*Resource{})

		subTask := NewTask(1, BasicFailAction{}, []*Task{}, []*Resource{})
		task.AddSubTask(subTask, OF_CONTINUE)

		// Run the parent task
		done := make(chan interface{})
		go func(chan<- interface{}) {
			task.Run(context.Background())
			done <- true
		}(done)

		select {
		case <-done:
			if task.GetStatus() != TS_SUCCESS {
				t.Error("Parent task did not succeed even though subtask had OF_CONTINUE")
			}
			return
		case <-time.After(10 * time.Millisecond):
			t.Errorf("Task did not finish in time")
			return
		}
	})

	// If a subtask with OF_ABORT fails, all other sibling tasks should be aborted
	t.Run("TestSiblingAbort", func(t *testing.T) {
		task := NewTask(0, BasicAction{}, []*Task{}, []*Resource{})

		subTask1 := NewTask(1, BasicFailAction{}, []*Task{}, []*Resource{})
		task.AddSubTask(subTask1, OF_ABORT)

		subTask2 := NewTask(2, BasicTimeoutAction{1 * time.Second}, []*Task{}, []*Resource{})
		task.AddSubTask(subTask2, OF_ABORT)

		// Run the parent task
		done := make(chan interface{})
		go func(chan<- interface{}) {
			task.Run(context.Background())
			done <- true
		}(done)

		select {
		case <-done:
			if task.GetStatus() != TS_FAILED {
				t.Error("Parent task did not fail after subtask failed")
			}
			if subTask2.GetStatus() != TS_FAILED {
				// TODO: This should be TS_ABORTED
				fmt.Printf("Subtask2 status: %d\n", subTask2.GetStatus())
				t.Error("Sibling task did not abort after sibling task failed")
			}
			return
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Task did not finish in time")
			return
		}
	})

	// Combine OF_ABORT and OF_CONTINUE for a tree of tasks
	t.Run("TestSubTaskAbortTree", func(t *testing.T) {
		// root
		// 	- task1 (OF_CONTINUE)
		// 		- task11 (OF_CONTINUE) - FAIL immediately
		// 		- task12 (OF_ABORT)
		// 			- task121 (OF_ABORT) - FAIL immediately
		// 	- task2 (OF_ABORT) - OK

		root := NewTask(0, BasicAction{}, []*Task{}, []*Resource{})
		task1 := NewTask(1, BasicAction{}, []*Task{}, []*Resource{})
		task11 := NewTask(11, BasicFailAction{}, []*Task{}, []*Resource{})
		task12 := NewTask(12, BasicAction{}, []*Task{}, []*Resource{})
		task121 := NewTask(121, BasicFailAction{}, []*Task{}, []*Resource{})
		task2 := NewTask(2, BasicTimeoutAction{10 * time.Millisecond}, []*Task{}, []*Resource{})
		root.AddSubTask(task1, OF_CONTINUE)
		task1.AddSubTask(task11, OF_CONTINUE)
		task1.AddSubTask(task12, OF_ABORT)
		task12.AddSubTask(task121, OF_ABORT)
		root.AddSubTask(task2, OF_ABORT)

		done := make(chan interface{})
		go func(chan<- interface{}) {
			root.Run(context.Background())
			done <- true
		}(done)

		select {
		case <-done:
			taskNames := []string{"root", "task1", "task11", "task12", "task121", "task2"}
			returnStatus := []TaskStatus{root.GetStatus(), task1.GetStatus(), task11.GetStatus(), task12.GetStatus(), task121.GetStatus(), task2.GetStatus()}
			expectedStatus := []TaskStatus{TS_SUCCESS, TS_FAILED, TS_FAILED, TS_FAILED, TS_FAILED, TS_SUCCESS}
			for i := 0; i < len(returnStatus); i++ {
				if returnStatus[i] != expectedStatus[i] {
					t.Errorf("Task %s returned status %s, expected %s", taskNames[i], returnStatus[i], expectedStatus[i])
					return
				}
			}
			return
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Task did not finish in time")
			return
		}
	})
}

func TestPrerequisites(t *testing.T) {
	// Task should not start until all prerequisites are met
	t.Run("TestWaitToStart", func(t *testing.T) {
		numPrerequisites := 10
		preRequisiteTimeout := 50 * time.Millisecond
		taskTimeout := 100 * time.Millisecond

		prerequisites := make([]*Task, numPrerequisites)

		for i := 0; i < numPrerequisites; i++ {
			// Each prerequisite should take a different amount of time to complete
			prerequisite := NewTask(0, BasicTimeoutAction{preRequisiteTimeout}, []*Task{}, []*Resource{})
			prerequisites[i] = prerequisite
		}

		task := NewTask(0, BasicAction{}, prerequisites, []*Resource{})

		// Start the task
		done := make(chan interface{})
		go func(chan<- interface{}) {
			task.Run(context.Background())
			done <- true
		}(done)

		// Start the prerequisites
		for _, prerequisite := range prerequisites {
			go prerequisite.Run(context.Background())
		}

		select {
		case <-done:
			if task.GetStatus() != TS_SUCCESS {
				t.Error("Task did not finish successfully")
				return
			}
			for _, prerequisite := range prerequisites {
				if prerequisite.GetStatus() != TS_SUCCESS {
					t.Error("Prerequisite did not finish successfully")
					return
				}
			}
			return
		case <-time.After(taskTimeout):
			t.Errorf("Task did not finish in time")
			return
		}

	})

	// If a prerequisite fails, the task should abort
	t.Run("TestPrerequisiteFail", func(t *testing.T) {
		prerequisites := []*Task{
			NewTask(0, BasicAction{}, []*Task{}, []*Resource{}),
			NewTask(0, BasicFailAction{}, []*Task{}, []*Resource{}),
		}
		task := NewTask(0, BasicAction{}, prerequisites, []*Resource{})

		done := make(chan interface{})
		go func(chan<- interface{}) {
			task.Run(context.Background())
			done <- true
		}(done)

		for _, prerequisite := range prerequisites {
			go prerequisite.Run(context.Background())
		}

		select {
		case <-done:
			if task.GetStatus() != TS_ABORTED {
				t.Error("Task did not abort even though a prerequisite failed")
				return
			}
			fmt.Printf("Task status: %s\n", task.GetStatus())
		case <-time.After(10 * time.Millisecond):
			t.Errorf("Task did not finish in time")
			return
		}
	})
}
