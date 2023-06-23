package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func TestTask2(t *testing.T) {
	pool := NewResourcePool()
	pool.CreateOrGetResource("scale", 3)
	MakeACake(NewTask2("Make a cake", pool))
}

func MakeACake(task *Task2) {
	// To bake a cake, we need to:
	// 1. Preheat the oven
	// 2. Measure the ingredients
	//    2.a Measure the flour
	//    2.b Measure the sugar
	// 3. Mix the ingredients
	// 4. Bake the cake
	// 5. (Non-critical) Decorate the cake

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
		preheatOven.Start()
		preheatOvenFunc(200)
		fmt.Println("Oven malfunction!")
		preheatOven.Complete(TS_SUCCESS)
	}()

	go func() {
		NewMeasureIngredientsTask(measureIngredients, "flour", "sugar", "eggs")
		measureIngredients.Start()
		measureIngredients.Complete(TS_SUCCESS)
	}()

	go func() {
		mixIngredients.Start()
		mixIngredients.Complete(TS_SUCCESS)
	}()

	go func() {
		bakeCake.Start()
		bakeCakeFunc(bakeCake)
		bakeCake.Complete(TS_SUCCESS)
	}()

	go func() {
		decorateCake.Start()
		decorateCakeFunc(decorateCake)
		decorateCake.Complete(TS_FAILED)
	}()

	task.Start()
	task.Complete(TS_SUCCESS)
}

func preheatOvenFunc(temp int) {
	fmt.Printf("Preheating oven to %d degrees\n", temp)
	time.Sleep(5 * time.Second)
}

func NewMeasureIngredientsTask(task *Task2, ingredients ...string) {
	for _, ingredient := range ingredients {
		ingredientTask := NewTask2(fmt.Sprintf("Measure %s", ingredient), task.ResourcePool)
		task.AddChild(ingredientTask, true)
		go func(t *Task2, ingredient string) {
			t.ResourcesRequired = append(t.ResourcesRequired, t.ResourcePool.CreateOrGetResource("scale", 1))
			t.Start()
			measure(ingredient)
			t.Complete(TS_SUCCESS)
		}(ingredientTask, ingredient)
	}
}

func measure(ingredient string) {
	time.Sleep(time.Second)
	fmt.Println("Measured", ingredient)
}

func bakeCakeFunc(task *Task2) {
	time.Sleep(10 * time.Second)
}

func decorateCakeFunc(task *Task2) {
	time.Sleep(time.Second)
}
