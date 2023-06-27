package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func TestTask2(t *testing.T) {
	pool := NewResourcePool()
	pool.CreateOrGetResource("scale", 2)
	MakeACake(NewTask2("Make a cake", pool, 0))
}

func MakeACake(task *Task2) {
	// To bake a cake, we need to:
	// - Preheat the oven
	// - Measure the ingredients (potentially having to wait for the shared scale)
	//    2.a Measure the flour
	//    2.b Measure the sugar
	//    2.x Measuse ...
	// - Mix the ingredients (once they are measured)
	// - Bake the cake (once the oven is preheated)
	// - (Non-critical) Decorate the cake (once it is baked)

	preheatOven := NewTask2("Preheat Oven", task.ResourcePool, 5)
	measureIngredients := NewTask2("Measure Ingredients", task.ResourcePool, 0)

	mixIngredients := NewTask2("Mix Ingredients", task.ResourcePool, 0)
	bakeCake := NewTask2("Bake Cake", task.ResourcePool, 0)
	decorateCake := NewTask2("Decorate Cake", task.ResourcePool, 0)

	// Set up relationships
	task.AddChild(preheatOven, true)
	task.AddChild(measureIngredients, true)
	measureIngredients.Then(mixIngredients, true, true)
	mixIngredients.Then(bakeCake, true, true)
	preheatOven.AddSuccessor(bakeCake, true)
	bakeCake.Then(decorateCake, true, false)

	// Start the task
	go func() {
		failsBeforeSuccess := 3
		try := 0
		preheatOven.StartWhenReady()
		for {
			select {
			case status := <-preheatOven.Interrupt:
				preheatOven.CompleteWhenReady(status)
				return
			case <-time.After(1 * time.Second):
				if try < failsBeforeSuccess {
					if preheatOven.Retry() {
						try++
						continue
					}
					preheatOven.CompleteWhenReady(TS_FAILED)
					return
				}
				preheatOven.CompleteWhenReady(TS_SUCCESS)
				return
			}
		}
	}()

	go func() {
		NewMeasureIngredientsTask(measureIngredients, "flour", "sugar", "eggs")
		measureIngredients.StartWhenReady()
		measureIngredients.CompleteWhenReady(TS_SUCCESS)
	}()

	go func() {
		mixIngredients.StartWhenReady()
		time.Sleep(3 * time.Second)
		mixIngredients.CompleteWhenReady(TS_SUCCESS)
	}()

	go func() {
		bakeCake.StartWhenReady()
		select {
		case status := <-bakeCake.Interrupt:
			bakeCake.CompleteWhenReady(status)
			return
		case <-time.After(5 * time.Second):
		}

		bakeCake.CompleteWhenReady(TS_SUCCESS)
	}()

	go func() {
		decorateCake.StartWhenReady()
		select {
		case status := <-decorateCake.Interrupt:
			decorateCake.CompleteWhenReady(status)
			return
		case <-time.After(2 * time.Second):
		}
		decorateCake.CompleteWhenReady(TS_FAILED)
	}()

	task.StartWhenReady()
	fmt.Println("Do not use the scale for the next 5 seconds! It must be calibrated.")
	task.ResourcePool.CreateOrGetResource("scale", 1).ProhibitUntil(time.Now().Add(5 * time.Second))

	task.CompleteWhenReady(TS_SUCCESS)
	time.Sleep(1 * time.Second)
	fmt.Print(task.logBuffer.String())
	fmt.Println("=========================================")
	fmt.Print(preheatOven.logBuffer.String())
	VisualizeTaskGraph(task)
}

func NewMeasureIngredientsTask(task *Task2, ingredients ...string) {
	for _, ingredient := range ingredients {
		ingredientTask := NewTask2(fmt.Sprintf("Measure %s", ingredient), task.ResourcePool, 0)
		task.AddChild(ingredientTask, true)
		go func(t *Task2, ingredient string) {
			t.resourcesRequired = append(t.resourcesRequired, t.ResourcePool.CreateOrGetResource("scale", 1))
			t.StartWhenReady()
			time.Sleep(time.Second)
			t.CompleteWhenReady(TS_SUCCESS)
		}(ingredientTask, ingredient)
	}
}
