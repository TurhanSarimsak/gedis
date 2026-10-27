// Package store is a sharded, concurrency-safe keyspace holding strings, lists
// and hashes, with millisecond-precision expiry.
package store

import (
	"errors"
	"maps"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"
)

var (
	ErrWrongType  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	ErrOverflow   = errors.New("ERR increment or decrement would overflow")
)

// Keys are spread over independent locks so commands on different keys rarely
// contend. Redis itself is single-threaded; this is where gedis differs.
const numShards = 64

type entry struct {
	value    any   // []byte, *list or map[string][]byte
	expireAt int64 // unix milliseconds; 0 means no TTL
}

type shard struct {
	mu       sync.Mutex
	data     map[string]*entry
	volatile map[string]struct{} // keys that have a TTL, sampled by ExpireCycle
}

type Store struct {
	shards [numShards]shard
	now    func() int64
}

func New() *Store {
	return NewWithClock(func() int64 { return time.Now().UnixMilli() })
}

// NewWithClock returns a store whose notion of "now" (unix ms) is controlled by the caller.
func NewWithClock(now func() int64) *Store {
	s := &Store{now: now}
	for i := range s.shards {
		s.shards[i].data = make(map[string]*entry)
		s.shards[i].volatile = make(map[string]struct{})
	}
	return s
}

// lock hashes key to its shard (FNV-1a, inlined so it never allocates) and locks it.
func (s *Store) lock(key string) *shard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	sh := &s.shards[h%numShards]
	sh.mu.Lock()
	return sh
}

// lookup returns the live entry for key. An expired entry is deleted on the
// spot (lazy expiry), so callers never observe it. Caller holds sh.mu.
func (sh *shard) lookup(key string, now int64) *entry {
	e := sh.data[key]
	if e != nil && e.expireAt != 0 && e.expireAt <= now {
		sh.remove(key)
		return nil
	}
	return e
}

func (sh *shard) remove(key string) {
	delete(sh.data, key)
	delete(sh.volatile, key)
}

func (sh *shard) setExpire(key string, e *entry, at int64) {
	e.expireAt = at
	if at == 0 {
		delete(sh.volatile, key)
	} else {
		sh.volatile[key] = struct{}{}
	}
}

// lookupAs fetches key's value as type T, or ErrWrongType if it holds something else.
func lookupAs[T any](sh *shard, key string, now int64) (T, bool, error) {
	var zero T
	e := sh.lookup(key, now)
	if e == nil {
		return zero, false, nil
	}
	v, ok := e.value.(T)
	if !ok {
		return zero, false, ErrWrongType
	}
	return v, true, nil
}

// ---- Keyspace ----

func (s *Store) Del(keys ...string) int {
	now, n := s.now(), 0
	for _, key := range keys {
		sh := s.lock(key)
		if sh.lookup(key, now) != nil {
			sh.remove(key)
			n++
		}
		sh.mu.Unlock()
	}
	return n
}

func (s *Store) Exists(keys ...string) int {
	now, n := s.now(), 0
	for _, key := range keys {
		sh := s.lock(key)
		if sh.lookup(key, now) != nil {
			n++
		}
		sh.mu.Unlock()
	}
	return n
}

func (s *Store) Type(key string) string {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	e := sh.lookup(key, s.now())
	if e == nil {
		return "none"
	}
	switch e.value.(type) {
	case []byte:
		return "string"
	case *list:
		return "list"
	default:
		return "hash"
	}
}

// Keys returns all live keys matching a glob pattern, sorted.
func (s *Store) Keys(pattern string) []string {
	now := s.now()
	var keys []string
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for key, e := range sh.data {
			if (e.expireAt == 0 || e.expireAt > now) && Match(pattern, key) {
				keys = append(keys, key)
			}
		}
		sh.mu.Unlock()
	}
	slices.Sort(keys)
	return keys
}

// Len counts keys, including expired ones not yet reclaimed (like Redis' DBSIZE).
func (s *Store) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		n += len(sh.data)
		sh.mu.Unlock()
	}
	return n
}

func (s *Store) Flush() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		clear(sh.data)
		clear(sh.volatile)
		sh.mu.Unlock()
	}
}

// ---- Expiry ----

// ExpireAt sets an absolute deadline in unix ms. A deadline in the past deletes
// the key, like Redis. Reports whether the key existed.
func (s *Store) ExpireAt(key string, at int64) bool {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	now := s.now()
	e := sh.lookup(key, now)
	if e == nil {
		return false
	}
	if at <= now {
		sh.remove(key)
	} else {
		sh.setExpire(key, e, at)
	}
	return true
}

func (s *Store) Persist(key string) bool {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	e := sh.lookup(key, s.now())
	if e == nil || e.expireAt == 0 {
		return false
	}
	sh.setExpire(key, e, 0)
	return true
}

// TTL returns the remaining time to live in ms, -1 if the key has no TTL and
// -2 if it does not exist.
func (s *Store) TTL(key string) int64 {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	now := s.now()
	e := sh.lookup(key, now)
	switch {
	case e == nil:
		return -2
	case e.expireAt == 0:
		return -1
	}
	return e.expireAt - now
}

// ExpireCycle reclaims expired keys that nobody reads again, mirroring Redis'
// active expiry: test a sample of 20 keys with a TTL per shard and repeat while
// more than 25% of the sample was expired. Go randomises map iteration order,
// which gives us the random sample for free. Returns the number of keys removed.
func (s *Store) ExpireCycle() int {
	const sampleSize = 20
	now, removed := s.now(), 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for range 16 { // bound the work done while holding the lock
			checked, expired := 0, 0
			for key := range sh.volatile {
				if checked == sampleSize {
					break
				}
				checked++
				if e := sh.data[key]; e == nil || e.expireAt <= now {
					sh.remove(key)
					expired++
				}
			}
			removed += expired
			if expired*4 <= checked {
				break
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// ---- Strings ----

func (s *Store) Get(key string) ([]byte, bool, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	return lookupAs[[]byte](sh, key, s.now())
}

type SetOptions struct {
	ExpireAt int64 // absolute unix ms; 0 means no TTL
	KeepTTL  bool
	NX       bool // only set if the key does not exist
	XX       bool // only set if the key exists
}

// Set stores value, replacing whatever the key held. Returns false when NX/XX
// prevented the write.
func (s *Store) Set(key string, value []byte, opt SetOptions) bool {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	e := sh.lookup(key, s.now())
	if (opt.NX && e != nil) || (opt.XX && e == nil) {
		return false
	}
	expireAt := opt.ExpireAt
	if opt.KeepTTL && e != nil {
		expireAt = e.expireAt
	}
	next := &entry{value: value}
	sh.data[key] = next
	sh.setExpire(key, next, expireAt)
	return true
}

// IncrBy adds delta to the integer stored at key, keeping its TTL.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	e := sh.lookup(key, s.now())

	var current int64
	if e != nil {
		b, ok := e.value.([]byte)
		if !ok {
			return 0, ErrWrongType
		}
		n, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		current = n
	}
	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	current += delta

	value := strconv.AppendInt(nil, current, 10)
	if e == nil {
		sh.data[key] = &entry{value: value}
	} else {
		e.value = value
	}
	return current, nil
}

// ---- Lists ----

// Push appends values to the head (left) or tail of the list at key, creating
// it if needed. Returns the new length.
func (s *Store) Push(key string, left bool, values ...[]byte) (int, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	l, ok, err := lookupAs[*list](sh, key, s.now())
	if err != nil {
		return 0, err
	}
	if !ok {
		l = &list{}
		sh.data[key] = &entry{value: l}
	}
	for _, v := range values {
		if left {
			l.pushFront(v) // LPUSH k a b c inserts one at a time, so the list reads c b a
		} else {
			l.pushBack(v)
		}
	}
	return l.len(), nil
}

// Pop removes up to count elements from the head or tail. An emptied list is
// deleted, as in Redis.
func (s *Store) Pop(key string, left bool, count int) ([][]byte, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	l, ok, err := lookupAs[*list](sh, key, s.now())
	if !ok || err != nil {
		return nil, err
	}

	out := make([][]byte, min(count, l.len()))
	for i := range out {
		if left {
			out[i] = l.popFront()
		} else {
			out[i] = l.popBack()
		}
	}
	if l.len() == 0 {
		sh.remove(key)
	}
	return out, nil
}

// Range returns elements start..stop inclusive; negative indexes count from the end.
func (s *Store) Range(key string, start, stop int64) ([][]byte, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	l, ok, err := lookupAs[*list](sh, key, s.now())
	if !ok || err != nil {
		return nil, err
	}

	n := int64(l.len())
	if start < 0 {
		start = max(start+n, 0)
	}
	if stop < 0 {
		stop += n
	}
	stop = min(stop, n-1)
	if start > stop {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, l.at(int(i)))
	}
	return out, nil
}

func (s *Store) ListLen(key string) (int, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	l, _, err := lookupAs[*list](sh, key, s.now())
	if l == nil {
		return 0, err
	}
	return l.len(), err
}

// ---- Hashes ----

// HSet sets field/value pairs and returns how many fields were newly added.
func (s *Store) HSet(key string, pairs ...[]byte) (int, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	h, ok, err := lookupAs[map[string][]byte](sh, key, s.now())
	if err != nil {
		return 0, err
	}
	if !ok {
		h = make(map[string][]byte, len(pairs)/2)
		sh.data[key] = &entry{value: h}
	}
	added := 0
	for i := 0; i+1 < len(pairs); i += 2 {
		field := string(pairs[i])
		if _, exists := h[field]; !exists {
			added++
		}
		h[field] = pairs[i+1]
	}
	return added, nil
}

func (s *Store) HGet(key, field string) ([]byte, bool, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	h, _, err := lookupAs[map[string][]byte](sh, key, s.now())
	v, ok := h[field]
	return v, ok, err
}

func (s *Store) HDel(key string, fields ...string) (int, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	h, ok, err := lookupAs[map[string][]byte](sh, key, s.now())
	if !ok || err != nil {
		return 0, err
	}
	n := 0
	for _, field := range fields {
		if _, exists := h[field]; exists {
			delete(h, field)
			n++
		}
	}
	if len(h) == 0 {
		sh.remove(key)
	}
	return n, nil
}

// HGetAll returns field, value, field, value... sorted by field.
func (s *Store) HGetAll(key string) ([][]byte, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	h, _, err := lookupAs[map[string][]byte](sh, key, s.now())
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(h)*2)
	for _, field := range slices.Sorted(maps.Keys(h)) {
		out = append(out, []byte(field), h[field])
	}
	return out, nil
}

func (s *Store) HLen(key string) (int, error) {
	sh := s.lock(key)
	defer sh.mu.Unlock()
	h, _, err := lookupAs[map[string][]byte](sh, key, s.now())
	return len(h), err
}
