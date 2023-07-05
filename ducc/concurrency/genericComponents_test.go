package concurrency

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestSync(t *testing.T) {
	Idcounter = 0
	in1 := NewRefCountedChan[TaggedValueWithCtx[int]]()
	in2 := NewRefCountedChan[TaggedValueWithCtx[int]]()
	in3 := NewRefCountedChan[TaggedValueWithCtx[int]]()
	syncer := NewSync[int](3)

	Connect(in1, syncer.In[0])
	Connect(in2, syncer.In[1])
	Connect(in3, syncer.In[2])

	fmt.Println("Connected all channels")

	seqLength := 10
	seq := make([]TaggedValueWithCtx[int], seqLength)
	for i := 0; i < seqLength; i++ {
		seq[i] = TaggedValueWithCtx[int]{
			TagStack: []Tag{{id: uuid.New()}},
			Value:    i,
		}
	}
	seq2 := make([]TaggedValueWithCtx[int], seqLength)
	seq3 := make([]TaggedValueWithCtx[int], seqLength)
	copy(seq2, seq)
	copy(seq3, seq)

	// Shuffle seq2 and seq3
	rand.Shuffle(len(seq2), func(i, j int) {
		seq2[i], seq2[j] = seq2[j], seq2[i]
	})
	rand.Shuffle(len(seq3), func(i, j int) {
		seq3[i], seq3[j] = seq3[j], seq3[i]
	})

	go syncer.Process()
	inputFunc := func(c RefCountedChan[TaggedValueWithCtx[int]], s []TaggedValueWithCtx[int]) {
		defer c.Close()
		fmt.Printf("Sending values to channel %d\n", c.id)
		for _, val := range s {
			c.ch <- val
		}
		fmt.Printf("Sent all values to channel %d\n", c.id)
	}
	go inputFunc(in1, seq)
	go inputFunc(in2, seq2)
	go inputFunc(in3, seq3)

	count := 0
	for {
		val1, ok1 := <-syncer.Out[0].ch
		val2, ok2 := <-syncer.Out[1].ch
		val3, ok3 := <-syncer.Out[2].ch

		// Channel closed
		if !ok1 {
			if ok2 || ok3 {
				t.Errorf("Not all channels closed simultaneously")
				return
			}
			break
		}
		if val1.TagStack[0] != val2.TagStack[0] || val1.TagStack[0] != val3.TagStack[0] {
			t.Errorf("Tags do not match")
		}
		count++
	}
	if count != seqLength {
		t.Errorf(fmt.Sprintf("Not all channels closed simultaneously, count = %d", count))
		return
	}
}

func TestY(t *testing.T) {
	a := NewRefCountedChan[string]()
	b := NewRefCountedChan[string]()
	c := NewRefCountedChan[string]()

	Connect(a, c)
	Connect(b, c)

	wg := sync.WaitGroup{}
	wg.Add(3)

	go func() {
		defer wg.Done()
		defer a.Close()
		a.ch <- "a"
	}()
	go func() {
		for v := range c.ch {
			fmt.Println(v)
		}
		wg.Done()
	}()
	go func() {
		defer wg.Done()
		defer b.Close()
		b.ch <- "b"
	}()
	wg.Wait()

}
