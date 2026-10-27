package store

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
)

type clock struct{ ms int64 }

func (c *clock) now() int64 { return c.ms }

func newTestStore() (*Store, *clock) {
	c := &clock{ms: 1_000_000}
	return NewWithClock(c.now), c
}

func join(values [][]byte) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = string(v)
	}
	return strings.Join(parts, ",")
}

func TestSetOptions(t *testing.T) {
	s, _ := newTestStore()
	if s.Set("k", []byte("1"), SetOptions{XX: true}) {
		t.Fatal("XX on a missing key must not write")
	}
	if !s.Set("k", []byte("1"), SetOptions{NX: true}) {
		t.Fatal("NX on a missing key must write")
	}
	if s.Set("k", []byte("2"), SetOptions{NX: true}) {
		t.Fatal("NX on an existing key must not write")
	}
	if v, _, _ := s.Get("k"); string(v) != "1" {
		t.Fatalf("got %q", v)
	}
}

func TestLazyExpiry(t *testing.T) {
	s, clk := newTestStore()
	s.Set("k", []byte("v"), SetOptions{ExpireAt: clk.ms + 100})
	if ttl := s.TTL("k"); ttl != 100 {
		t.Fatalf("TTL = %d, want 100", ttl)
	}
	clk.ms += 100
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("key should have expired")
	}
	if ttl := s.TTL("k"); ttl != -2 {
		t.Fatalf("TTL of missing key = %d, want -2", ttl)
	}
}

func TestKeepTTLAndPersist(t *testing.T) {
	s, clk := newTestStore()
	s.Set("k", []byte("a"), SetOptions{ExpireAt: clk.ms + 500})
	s.Set("k", []byte("b"), SetOptions{KeepTTL: true})
	if ttl := s.TTL("k"); ttl != 500 {
		t.Fatalf("KEEPTTL lost the TTL: %d", ttl)
	}
	s.IncrBy("n", 1)
	if !s.Persist("k") || s.TTL("k") != -1 {
		t.Fatal("PERSIST should clear the TTL")
	}
}

func TestExpireInThePastDeletes(t *testing.T) {
	s, clk := newTestStore()
	s.Set("k", []byte("v"), SetOptions{})
	if !s.ExpireAt("k", clk.ms-1) {
		t.Fatal("ExpireAt should report the key existed")
	}
	if s.Exists("k") != 0 {
		t.Fatal("key should be gone")
	}
}

func TestActiveExpiryReclaimsUnreadKeys(t *testing.T) {
	s, clk := newTestStore()
	for i := range 10_000 {
		s.Set(fmt.Sprint("tmp:", i), []byte("x"), SetOptions{ExpireAt: clk.ms + 10})
	}
	s.Set("keep", []byte("x"), SetOptions{})
	clk.ms += 10

	for range 100 {
		if s.ExpireCycle() == 0 {
			break
		}
	}
	if n := s.Len(); n != 1 {
		t.Fatalf("Len = %d after expiry cycles, want 1", n)
	}
}

func TestIncrBy(t *testing.T) {
	s, _ := newTestStore()
	if n, _ := s.IncrBy("n", 5); n != 5 {
		t.Fatalf("got %d", n)
	}
	if n, _ := s.IncrBy("n", -7); n != -2 {
		t.Fatalf("got %d", n)
	}
	s.Set("max", []byte(fmt.Sprint(int64(math.MaxInt64))), SetOptions{})
	if _, err := s.IncrBy("max", 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
	s.Set("text", []byte("abc"), SetOptions{})
	if _, err := s.IncrBy("text", 1); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("expected ErrNotInteger, got %v", err)
	}
}

func TestWrongType(t *testing.T) {
	s, _ := newTestStore()
	s.Push("l", false, []byte("a"))
	if _, _, err := s.Get("l"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("GET on a list: %v", err)
	}
	if _, err := s.HSet("l", []byte("f"), []byte("v")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HSET on a list: %v", err)
	}
}

func TestLists(t *testing.T) {
	s, _ := newTestStore()
	s.Push("l", true, []byte("a"), []byte("b"), []byte("c")) // c b a
	s.Push("l", false, []byte("d"), []byte("e"))             // c b a d e

	for _, tc := range []struct {
		start, stop int64
		want        string
	}{
		{0, -1, "c,b,a,d,e"},
		{1, 2, "b,a"},
		{-2, -1, "d,e"},
		{-100, 100, "c,b,a,d,e"},
		{3, 1, ""},
		{10, 20, ""},
	} {
		got, _ := s.Range("l", tc.start, tc.stop)
		if join(got) != tc.want {
			t.Errorf("Range(%d, %d) = %q, want %q", tc.start, tc.stop, join(got), tc.want)
		}
	}

	if got, _ := s.Pop("l", true, 2); join(got) != "c,b" {
		t.Fatalf("LPOP 2 = %q", join(got))
	}
	if got, _ := s.Pop("l", false, 10); join(got) != "e,d,a" {
		t.Fatalf("RPOP 10 = %q", join(got))
	}
	if s.Exists("l") != 0 {
		t.Fatal("empty list should be deleted")
	}
}

func TestHashes(t *testing.T) {
	s, _ := newTestStore()
	if n, _ := s.HSet("h", []byte("b"), []byte("2"), []byte("a"), []byte("1")); n != 2 {
		t.Fatalf("added %d", n)
	}
	if n, _ := s.HSet("h", []byte("a"), []byte("one")); n != 0 {
		t.Fatalf("overwriting a field should add 0, got %d", n)
	}
	if all, _ := s.HGetAll("h"); join(all) != "a,one,b,2" {
		t.Fatalf("HGETALL = %q", join(all))
	}
	if n, _ := s.HDel("h", "a", "b", "missing"); n != 2 {
		t.Fatalf("HDEL removed %d", n)
	}
	if s.Type("h") != "none" {
		t.Fatal("empty hash should be deleted")
	}
}

func TestKeysGlob(t *testing.T) {
	s, _ := newTestStore()
	for _, k := range []string{"user:1", "user:2", "user:10/profile", "session:1"} {
		s.Set(k, []byte("x"), SetOptions{})
	}
	if got := strings.Join(s.Keys("user:*"), ","); got != "user:1,user:10/profile,user:2" {
		t.Fatalf("Keys = %q", got)
	}
}

func TestMatch(t *testing.T) {
	for _, tc := range []struct {
		pattern, str string
		want         bool
	}{
		{"*", "", true},
		{"*", "a/b", true},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "heeeello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-b]llo", "hbllo", true},
		{"h[z-a]llo", "hmllo", true},
		{`h\*llo`, "h*llo", true},
		{`h\*llo`, "hello", false},
		{"[abc", "[abc", true},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXbYY", false},
	} {
		if got := Match(tc.pattern, tc.str); got != tc.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.str, got, tc.want)
		}
	}
}

// Run with -race: hammering one key from many goroutines must not lose updates.
func TestConcurrentIncr(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				s.IncrBy("counter", 1)
			}
		}()
	}
	wg.Wait()
	if v, _, _ := s.Get("counter"); string(v) != "50000" {
		t.Fatalf("counter = %s, want 50000", v)
	}
}

func BenchmarkSetGetParallel(b *testing.B) {
	s := New()
	value := []byte("value")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprint("key:", i%10_000)
			s.Set(key, value, SetOptions{})
			s.Get(key)
			i++
		}
	})
}
