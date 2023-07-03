package concurrency

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

func TestSync(t *testing.T) {
	in1 := make(chan TaggedValueWithCtx[int])
	in2 := make(chan TaggedValueWithCtx[int])
	in3 := make(chan TaggedValueWithCtx[int])

	syncer := NewSync[int](3)

	Connect(in1, syncer.In[0])
	Connect(in2, syncer.In[1])
	Connect(in3, syncer.In[2])

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
	inputFunc := func(c chan TaggedValueWithCtx[int], s []TaggedValueWithCtx[int]) {
		defer close(c)
		for _, val := range s {
			c <- val
		}
	}
	go inputFunc(in1, seq)
	go inputFunc(in2, seq2)
	go inputFunc(in3, seq3)

	for {
		val1, ok1 := <-syncer.Out[0]
		val2, ok2 := <-syncer.Out[1]
		val3, ok3 := <-syncer.Out[2]

		if !ok1 {
			if !ok2 && !ok3 {
				return
			} else {
				t.Errorf("Not all channels closed simultaneously")
			}
		}
		if val1.TagStack[0] != val2.TagStack[0] || val1.TagStack[0] != val3.TagStack[0] {
			t.Errorf("Tags do not match")
		}
	}
}
