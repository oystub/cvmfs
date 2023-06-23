package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func TestTask2(t *testing.T) {
	pool := NewResourcePool()
	pool.CreateOrGetResource("scale", 2)
	MakeACake(NewTask2("Make a cake", pool))
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

	preheatOven := NewTask2("Preheat Oven", task.ResourcePool)
	measureIngredients := NewTask2("Measure Ingredients", task.ResourcePool)

	mixIngredients := NewTask2("Mix Ingredients", task.ResourcePool)
	bakeCake := NewTask2("Bake Cake", task.ResourcePool)
	decorateCake := NewTask2("Decorate Cake", task.ResourcePool)

	// Set up relationships
	task.AddChild(preheatOven, true)
	task.AddChild(measureIngredients, true)
	measureIngredients.Then(mixIngredients, true, true)
	mixIngredients.Then(bakeCake, true, true)
	preheatOven.AddSuccessor(bakeCake, true)
	bakeCake.Then(decorateCake, true, false)

	// Start the task
	go func() {
		preheatOven.StartWhenReady()
		time.Sleep(10 * time.Second)
		preheatOven.Complete(TS_SUCCESS)
	}()

	go func() {
		NewMeasureIngredientsTask(measureIngredients, "flour", "sugar", "eggs")
		measureIngredients.StartWhenReady()
		measureIngredients.Complete(TS_SUCCESS)
	}()

	go func() {
		mixIngredients.StartWhenReady()
		time.Sleep(3 * time.Second)
		mixIngredients.Complete(TS_SUCCESS)
	}()

	go func() {
		bakeCake.StartWhenReady()
		time.Sleep(5 * time.Second)
		bakeCake.Complete(TS_SUCCESS)
	}()

	go func() {
		decorateCake.StartWhenReady()
		time.Sleep(2 * time.Second)
		decorateCake.Complete(TS_FAILED)
	}()

	task.StartWhenReady()
	task.Complete(TS_SUCCESS)
}

func NewMeasureIngredientsTask(task *Task2, ingredients ...string) {
	for _, ingredient := range ingredients {
		ingredientTask := NewTask2(fmt.Sprintf("Measure %s", ingredient), task.ResourcePool)
		task.AddChild(ingredientTask, true)
		go func(t *Task2, ingredient string) {
			t.ResourcesRequired = append(t.ResourcesRequired, t.ResourcePool.CreateOrGetResource("scale", 1))
			t.StartWhenReady()
			time.Sleep(time.Second)
			t.Complete(TS_SUCCESS)
		}(ingredientTask, ingredient)
	}
}
