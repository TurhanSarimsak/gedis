package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TurhanSarimsak/gedis/internal/aof"
)

// errorReply distinguishes "-ERR ..." from "+OK" in decoded replies.
type errorReply string

func readReply(br *bufio.Reader) (any, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return errorReply(line[1:]), nil
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		items := make([]any, n)
		for i := range items {
			if items[i], err = readReply(br); err != nil {
				return nil, err
			}
		}
		return items, nil
	}
	return nil, fmt.Errorf("unexpected reply %q", line)
}

type testClient struct {
	t    testing.TB
	conn net.Conn
	br   *bufio.Reader
}

func encode(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

func (tc *testClient) send(args ...string) {
	tc.t.Helper()
	if _, err := tc.conn.Write(encode(args...)); err != nil {
		tc.t.Fatal(err)
	}
}

func (tc *testClient) read() any {
	tc.t.Helper()
	tc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reply, err := readReply(tc.br)
	if err != nil {
		tc.t.Fatal(err)
	}
	return reply
}

func (tc *testClient) do(args ...string) any {
	tc.t.Helper()
	tc.send(args...)
	return tc.read()
}

func (tc *testClient) expect(want any, args ...string) {
	tc.t.Helper()
	if got := tc.do(args...); !reflect.DeepEqual(got, want) {
		tc.t.Fatalf("%v\n got: %#v\nwant: %#v", args, got, want)
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func startServer(t testing.TB, cfg Config) (*Server, string) {
	t.Helper()
	cfg.Logger = quietLogger()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ln)
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	return s, ln.Addr().String()
}

func dial(t testing.TB, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{t: t, conn: conn, br: bufio.NewReader(conn)}
}

func TestStringsAndKeys(t *testing.T) {
	_, addr := startServer(t, Config{})
	c := dial(t, addr)

	c.expect("PONG", "PING")
	c.expect("OK", "SET", "name", "turhan")
	c.expect("turhan", "GET", "name")
	c.expect(nil, "SET", "name", "x", "NX")
	c.expect(nil, "GET", "missing")
	c.expect(int64(11), "INCRBY", "n", "11")
	c.expect(int64(10), "DECR", "n")
	c.expect(errorReply("ERR value is not an integer or out of range"), "INCR", "name")
	c.expect([]any{"turhan", nil, "10"}, "MGET", "name", "missing", "n")
	c.expect(int64(2), "EXISTS", "name", "n", "missing")
	c.expect([]any{"n", "name"}, "KEYS", "n*")
	c.expect("string", "TYPE", "name")
	c.expect(int64(2), "DEL", "name", "n", "missing")
	c.expect(int64(0), "DBSIZE")
}

func TestExpiry(t *testing.T) {
	_, addr := startServer(t, Config{})
	c := dial(t, addr)

	c.expect("OK", "SET", "k", "v", "PX", "50")
	if ttl := c.do("PTTL", "k").(int64); ttl <= 0 || ttl > 50 {
		t.Fatalf("PTTL = %d", ttl)
	}
	time.Sleep(60 * time.Millisecond)
	c.expect(nil, "GET", "k")
	c.expect(int64(-2), "TTL", "k")

	c.expect("OK", "SET", "k", "v")
	c.expect(int64(-1), "TTL", "k")
	c.expect(int64(1), "EXPIRE", "k", "100")
	c.expect(int64(100), "TTL", "k")
	c.expect(int64(1), "PERSIST", "k")
	c.expect(int64(-1), "TTL", "k")
	c.expect(errorReply("ERR invalid expire time in 'set' command"), "SET", "k", "v", "EX", "0")
	c.expect(errorReply("ERR syntax error"), "SET", "k", "v", "NX", "XX")
}

func TestListsAndHashes(t *testing.T) {
	_, addr := startServer(t, Config{})
	c := dial(t, addr)

	c.expect(int64(3), "RPUSH", "q", "a", "b", "c")
	c.expect(int64(4), "LPUSH", "q", "z")
	c.expect([]any{"z", "a", "b", "c"}, "LRANGE", "q", "0", "-1")
	c.expect("z", "LPOP", "q")
	c.expect([]any{"c", "b"}, "RPOP", "q", "2")
	c.expect(int64(1), "LLEN", "q")
	c.expect(nil, "LPOP", "nope")

	c.expect(int64(2), "HSET", "user:1", "name", "turhan", "age", "20")
	c.expect("turhan", "HGET", "user:1", "name")
	c.expect([]any{"age", "20", "name", "turhan"}, "HGETALL", "user:1")
	c.expect(int64(1), "HDEL", "user:1", "age")
	c.expect(int64(1), "HLEN", "user:1")
	c.expect(errorReply("WRONGTYPE Operation against a key holding the wrong kind of value"), "GET", "user:1")
}

func TestErrors(t *testing.T) {
	_, addr := startServer(t, Config{})
	c := dial(t, addr)

	c.expect(errorReply("ERR unknown command 'NOPE'"), "NOPE")
	c.expect(errorReply("ERR wrong number of arguments for 'get' command"), "GET")
	c.expect("PONG", "PING") // connection still usable after errors
}

func TestInlineCommands(t *testing.T) {
	_, addr := startServer(t, Config{})
	c := dial(t, addr)
	c.conn.Write([]byte("SET greeting hello\r\nGET greeting\r\n"))
	if got := c.read(); got != "OK" {
		t.Fatalf("got %#v", got)
	}
	if got := c.read(); got != "hello" {
		t.Fatalf("got %#v", got)
	}
}

func TestPipelining(t *testing.T) {
	_, addr := startServer(t, Config{})
	c := dial(t, addr)

	var batch bytes.Buffer
	for i := range 1000 {
		batch.Write(encode("SET", fmt.Sprint("key:", i), fmt.Sprint(i)))
	}
	batch.Write(encode("DBSIZE"))
	c.conn.Write(batch.Bytes())
	for range 1000 {
		if got := c.read(); got != "OK" {
			t.Fatalf("got %#v", got)
		}
	}
	if got := c.read(); got != int64(1000) {
		t.Fatalf("DBSIZE = %#v", got)
	}
}

func TestTransactions(t *testing.T) {
	_, addr := startServer(t, Config{})
	c := dial(t, addr)

	c.expect("OK", "MULTI")
	c.expect("QUEUED", "SET", "balance", "100")
	c.expect("QUEUED", "INCRBY", "balance", "-30")
	c.expect("QUEUED", "GET", "balance")
	c.expect([]any{"OK", int64(70), "70"}, "EXEC")

	// A bad command while queueing aborts the whole transaction.
	c.expect("OK", "MULTI")
	c.expect("QUEUED", "SET", "balance", "0")
	c.expect(errorReply("ERR wrong number of arguments for 'get' command"), "GET")
	c.expect(errorReply("EXECABORT Transaction discarded because of previous errors."), "EXEC")
	c.expect("70", "GET", "balance")

	c.expect(errorReply("ERR EXEC without MULTI"), "EXEC")
}

// Transactions must be atomic: concurrent readers never see a half-applied transfer.
func TestTransactionAtomicity(t *testing.T) {
	_, addr := startServer(t, Config{})
	setup := dial(t, addr)
	setup.expect("OK", "MSET", "a", "100", "b", "0")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := dial(t, addr)
		for range 200 {
			w.send("MULTI")
			w.send("DECRBY", "a", "1")
			w.send("INCRBY", "b", "1")
			w.send("EXEC")
			for range 4 {
				w.read()
			}
		}
	}()

	r := dial(t, addr)
	for range 200 {
		r.send("MULTI")
		r.send("MGET", "a", "b")
		r.send("EXEC")
		r.read()
		r.read()
		pair := r.read().([]any)[0].([]any)
		a, _ := strconv.Atoi(pair[0].(string))
		b, _ := strconv.Atoi(pair[1].(string))
		if a+b != 100 {
			t.Fatalf("observed torn transaction: a=%d b=%d", a, b)
		}
	}
	wg.Wait()
}

func TestPubSub(t *testing.T) {
	_, addr := startServer(t, Config{})
	sub := dial(t, addr)
	pub := dial(t, addr)

	sub.expect([]any{"subscribe", "news", int64(1)}, "SUBSCRIBE", "news")
	sub.expect(errorReply("ERR Can't execute 'get': only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are allowed in this context"), "GET", "x")

	pub.expect(int64(1), "PUBLISH", "news", "hello")
	if got := sub.read(); !reflect.DeepEqual(got, []any{"message", "news", "hello"}) {
		t.Fatalf("got %#v", got)
	}
	pub.expect(int64(0), "PUBLISH", "other", "ignored")

	sub.expect([]any{"unsubscribe", "news", int64(0)}, "UNSUBSCRIBE")
	sub.expect("OK", "SET", "x", "1") // back to normal mode
}

func TestAOFPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := Config{AOFPath: path, AOFPolicy: aof.Always}

	s, addr := startServer(t, cfg)
	c := dial(t, addr)
	c.expect("OK", "SET", "user", "turhan")
	c.expect("OK", "SET", "session", "abc", "EX", "1000")
	c.expect("OK", "SET", "gone", "soon", "PX", "1")
	c.expect(int64(3), "RPUSH", "list", "a", "b", "c")
	c.expect(int64(5), "INCRBY", "counter", "5")
	c.expect(int64(1), "HSET", "h", "f", "v")
	c.expect(int64(1), "DEL", "h")
	c.conn.Close()
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash in the middle of writing a command.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("*3\r\n$3\r\nSET\r\n$4\r\nhalf")
	f.Close()

	time.Sleep(5 * time.Millisecond) // let "gone" expire
	_, addr = startServer(t, cfg)
	c = dial(t, addr)
	c.expect("turhan", "GET", "user")
	c.expect([]any{"a", "b", "c"}, "LRANGE", "list", "0", "-1")
	c.expect("5", "GET", "counter")
	c.expect(int64(0), "EXISTS", "h", "gone")
	if ttl := c.do("TTL", "session").(int64); ttl < 990 || ttl > 1000 {
		t.Fatalf("TTL after restart = %d; relative TTLs must be logged as absolute", ttl)
	}
	c.expect("OK", "SET", "after", "crash") // appends after the truncated tail must be readable
	c.expect("crash", "GET", "after")
}

func BenchmarkPipelinedSet(b *testing.B) {
	_, addr := startServer(b, Config{})
	c := dial(b, addr)
	const depth = 100
	var batch bytes.Buffer
	for i := range depth {
		batch.Write(encode("SET", fmt.Sprint("key:", i), "value"))
	}
	payload := batch.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i += depth {
		c.conn.Write(payload)
		for range depth {
			c.read()
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}
