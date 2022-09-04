package purekv

// Impl. source: https://gist.github.com/nwillc/554847806891a41e7bd32041308dfb40#file-go_generics_heap-go

// Heap holds generic heap implementation
type Heap[T any] struct {
	Data []T
	Comp func(a, b T) bool
}

// NewHeap creates new instance of Heap, by comparator
func NewHeap[T any](comp func(a, b T) bool) *Heap[T] {
	return &Heap[T]{Comp: comp}
}

// Len returns size of Heap
func (h *Heap[T]) Len() int { return len(h.Data) }

// Push adds new element to Heap
func (h *Heap[T]) Push(v T) {
	h.Data = append(h.Data, v)
	h.up(h.Len() - 1)
}

// ViewTop returns the element from the top of the heap, without removing it
func (h *Heap[T]) ViewTop() T {
	return h.Data[0]
}

// Pop removes and returns top element from Heap
func (h *Heap[T]) Pop() T {
	n := h.Len() - 1
	if n > 0 {
		h.swap(0, n)
		h.down()
	}
	v := h.Data[n]
	h.Data = h.Data[0:n]
	return v
}

func (h *Heap[T]) swap(i, j int) {
	h.Data[i], h.Data[j] = h.Data[j], h.Data[i]
}

func (h *Heap[T]) up(jj int) {
	for {
		i := parent(jj)
		if i == jj || !h.Comp(h.Data[jj], h.Data[i]) {
			break
		}
		h.swap(i, jj)
		jj = i
	}
}

func (h *Heap[T]) down() {
	n := h.Len() - 1
	i1 := 0
	for {
		j1 := left(i1)
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		j2 := right(i1)
		if j2 < n && h.Comp(h.Data[j2], h.Data[j1]) {
			j = j2
		}
		if !h.Comp(h.Data[j], h.Data[i1]) {
			break
		}
		h.swap(i1, j)
		i1 = j
	}
}

func parent(i int) int { return (i - 1) / 2 }
func left(i int) int   { return (i * 2) + 1 }
func right(i int) int  { return left(i) + 1 }
