package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestResources(t *testing.T) {
	// If a task requires a resource, it should not start until the resource is available
	t.Run("TestWaitForResource", func(t *testing.T) {
		resourceTimeout := 50 * time.Millisecond
		testTimeout := 100 * time.Millisecond

		pool := NewResourcePool()
		resource := pool.CreateOrGetResource("test", 1)

		task := NewTask(0, BasicAction{}, []*Task{}, []*Resource{resource})

		resource.Acquire()
		resourceUnlocked := make(chan interface{})
		// Start the resource
		go func(done chan<- interface{}) {
			time.Sleep(resourceTimeout)
			resource.Release()
			done <- true
		}(resourceUnlocked)

		// Start the task
		done := make(chan interface{})
		go func(chan<- interface{}) {
			task.Run(context.Background())
			done <- true
		}(done)

		tooEarly := true
		select {
		case <-resourceUnlocked:
			tooEarly = false
			break
		case <-done:
			if tooEarly {
				t.Error("Task finished before resource was released")
			}
			if task.GetStatus() != TS_SUCCESS {
				t.Error("Task did not finish successfully")
			}
			return
		case <-time.After(testTimeout):
			t.Errorf("Task did not finish in time")
			return
		}
	})

	t.Run("TestAcquireMultiple", func(t *testing.T) {
		numResources := 100
		pool := NewResourcePool()

		resources := make([]*Resource, numResources)
		for i := 0; i < numResources; i++ {
			resources[i] = pool.CreateOrGetResource(fmt.Sprintf("test%d", i), 1)
		}

		tasks := make([]*Task, numResources)
		for i := 0; i < numResources; i++ {
			// Each task needs all resources except for the one with the same index
			var taskResources []*Resource
			for j := 0; j < numResources; j++ {
				if j != i {
					taskResources = append(taskResources, resources[j])
				}
			}
			tasks[i] = NewTask(0, BasicAction{}, []*Task{}, taskResources)
		}

		// Start all tasks
		done := make(chan interface{})

		for _, task := range tasks {
			go func(done chan<- interface{}, cur_task *Task) {
				cur_task.Run(context.Background())
				done <- true
			}(done, task)
		}

		remaining := numResources
		for {
			select {
			case <-done:
				remaining--
				if remaining == 0 {
					return
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Task did not finish in time")
				return
			}
		}
	})

}
