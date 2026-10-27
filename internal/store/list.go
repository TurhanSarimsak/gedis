package store

// list is a growable ring buffer: O(1) amortised push and pop at both ends.
// A plain slice makes every LPUSH shift the whole list, which turns a stream
// of LPUSHes to one key quadratic (redis-benchmark measured ~1.3k ops/s).
type list struct {
	buf  [][]byte
	head int // index of the first element
	n    int // number of elements
}

func (l *list) len() int { return l.n }

// at returns the i-th element from the front, 0 <= i < n.
func (l *list) at(i int) []byte { return l.buf[(l.head+i)%len(l.buf)] }

func (l *list) grow() {
	next := make([][]byte, max(8, len(l.buf)*2))
	for i := range l.n {
		next[i] = l.at(i)
	}
	l.buf, l.head = next, 0
}

func (l *list) pushFront(v []byte) {
	if l.n == len(l.buf) {
		l.grow()
	}
	l.head = (l.head - 1 + len(l.buf)) % len(l.buf)
	l.buf[l.head] = v
	l.n++
}

func (l *list) pushBack(v []byte) {
	if l.n == len(l.buf) {
		l.grow()
	}
	l.buf[(l.head+l.n)%len(l.buf)] = v
	l.n++
}

func (l *list) popFront() []byte {
	v := l.buf[l.head]
	l.buf[l.head] = nil // drop the reference so the value can be collected
	l.head = (l.head + 1) % len(l.buf)
	l.n--
	return v
}

func (l *list) popBack() []byte {
	i := (l.head + l.n - 1) % len(l.buf)
	v := l.buf[i]
	l.buf[i] = nil
	l.n--
	return v
}
