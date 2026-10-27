package server

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/TurhanSarimsak/gedis/internal/store"
)

// handler executes a command and writes its reply. It returns the command to
// append to the AOF (possibly rewritten, e.g. relative TTLs made absolute), or
// nil if nothing needs to be persisted.
type handler func(s *Server, c *client, args [][]byte) (propagate [][]byte)

type command struct {
	handler handler
	arity   int  // like Redis: N means exactly N args (including the name), -N at least N
	write   bool // modifies the dataset
	noLock  bool // never touches the store, or manages txLock itself (EXEC)
}

func (cmd command) arityOK(n int) bool {
	if cmd.arity >= 0 {
		return n == cmd.arity
	}
	return n >= -cmd.arity
}

// Filled in init() because EXEC looks commands up in this table.
var commands map[string]command

func init() {
	commands = map[string]command{
		"PING":    {handler: cmdPing, arity: -1, noLock: true},
		"ECHO":    {handler: cmdEcho, arity: 2, noLock: true},
		"INFO":    {handler: cmdInfo, arity: -1},
		"COMMAND": {handler: cmdCommand, arity: -1, noLock: true},
		"CLIENT":  {handler: cmdOK, arity: -2, noLock: true},
		"SELECT":  {handler: cmdSelect, arity: 2, noLock: true},

		"GET":    {handler: cmdGet, arity: 2},
		"SET":    {handler: cmdSet, arity: -3, write: true},
		"MGET":   {handler: cmdMGet, arity: -2},
		"MSET":   {handler: cmdMSet, arity: -3, write: true},
		"INCR":   {handler: incrBy(1), arity: 2, write: true},
		"DECR":   {handler: incrBy(-1), arity: 2, write: true},
		"INCRBY": {handler: incrBy(0), arity: 3, write: true},
		"DECRBY": {handler: incrBy(0), arity: 3, write: true},

		"DEL":       {handler: cmdDel, arity: -2, write: true},
		"EXISTS":    {handler: cmdExists, arity: -2},
		"TYPE":      {handler: cmdType, arity: 2},
		"KEYS":      {handler: cmdKeys, arity: 2},
		"DBSIZE":    {handler: cmdDBSize, arity: 1},
		"FLUSHALL":  {handler: cmdFlushAll, arity: -1, write: true},
		"FLUSHDB":   {handler: cmdFlushAll, arity: -1, write: true},
		"EXPIRE":    {handler: expire(time.Second, false), arity: 3, write: true},
		"PEXPIRE":   {handler: expire(time.Millisecond, false), arity: 3, write: true},
		"EXPIREAT":  {handler: expire(time.Second, true), arity: 3, write: true},
		"PEXPIREAT": {handler: expire(time.Millisecond, true), arity: 3, write: true},
		"TTL":       {handler: ttl(time.Second), arity: 2},
		"PTTL":      {handler: ttl(time.Millisecond), arity: 2},
		"PERSIST":   {handler: cmdPersist, arity: 2, write: true},

		"LPUSH":  {handler: push(true), arity: -3, write: true},
		"RPUSH":  {handler: push(false), arity: -3, write: true},
		"LPOP":   {handler: pop(true), arity: -2, write: true},
		"RPOP":   {handler: pop(false), arity: -2, write: true},
		"LRANGE": {handler: cmdLRange, arity: 4},
		"LLEN":   {handler: cmdLLen, arity: 2},

		"HSET":    {handler: cmdHSet, arity: -4, write: true},
		"HGET":    {handler: cmdHGet, arity: 3},
		"HDEL":    {handler: cmdHDel, arity: -3, write: true},
		"HGETALL": {handler: cmdHGetAll, arity: 2},
		"HLEN":    {handler: cmdHLen, arity: 2},

		"MULTI":   {handler: cmdMulti, arity: 1, noLock: true},
		"EXEC":    {handler: cmdExec, arity: 1, noLock: true},
		"DISCARD": {handler: cmdDiscard, arity: 1, noLock: true},

		// Pub/sub commands must not take txLock: a subscriber holds its own
		// client lock while running them, and EXEC may PUBLISH to it while
		// holding txLock. Taking txLock here would deadlock the two.
		"PUBLISH":     {handler: cmdPublish, arity: 3, noLock: true},
		"SUBSCRIBE":   {handler: cmdSubscribe, arity: -2, noLock: true},
		"UNSUBSCRIBE": {handler: cmdUnsubscribe, arity: -1, noLock: true},
	}
}

const (
	errSyntax     = "ERR syntax error"
	errNotInteger = "ERR value is not an integer or out of range"
)

func nowMs() int64 { return time.Now().UnixMilli() }

func parseInt(b []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

func itob(n int64) []byte { return strconv.AppendInt(nil, n, 10) }

func replyErr(c *client, err error) [][]byte {
	c.w.Error(err.Error())
	return nil
}

func writeArray(c *client, values [][]byte) {
	c.w.ArrayHeader(len(values))
	for _, v := range values {
		c.w.Bulk(v)
	}
}

// ---- Connection ----

func cmdPing(_ *Server, c *client, args [][]byte) [][]byte {
	if len(args) > 1 {
		c.w.Bulk(args[1])
	} else {
		c.w.SimpleString("PONG")
	}
	return nil
}

func cmdEcho(_ *Server, c *client, args [][]byte) [][]byte {
	c.w.Bulk(args[1])
	return nil
}

func cmdOK(_ *Server, c *client, _ [][]byte) [][]byte {
	c.w.SimpleString("OK")
	return nil
}

// cmdCommand answers the COMMAND / COMMAND DOCS probe redis-cli sends on connect.
func cmdCommand(_ *Server, c *client, _ [][]byte) [][]byte {
	c.w.ArrayHeader(0)
	return nil
}

func cmdSelect(_ *Server, c *client, args [][]byte) [][]byte {
	if string(args[1]) != "0" {
		c.w.Error("ERR gedis has a single database (0)")
		return nil
	}
	c.w.SimpleString("OK")
	return nil
}

func cmdInfo(s *Server, c *client, _ [][]byte) [][]byte {
	s.mu.Lock()
	clients := len(s.clients)
	s.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\nredis_version:7.2.0\r\ngedis_version:%s\r\nuptime_in_seconds:%d\r\n\r\n",
		Version, int(time.Since(s.startedAt).Seconds()))
	fmt.Fprintf(&b, "# Clients\r\nconnected_clients:%d\r\n\r\n", clients)
	fmt.Fprintf(&b, "# Persistence\r\naof_enabled:%d\r\n\r\n", map[bool]int{false: 0, true: 1}[s.aof != nil])
	fmt.Fprintf(&b, "# Stats\r\ntotal_connections_received:%d\r\ntotal_commands_processed:%d\r\n\r\n",
		s.totalConns.Load(), s.totalCommands.Load())
	fmt.Fprintf(&b, "# Keyspace\r\ndb0:keys=%d\r\n", s.store.Len())
	c.w.BulkString(b.String())
	return nil
}

// ---- Strings ----

func cmdGet(s *Server, c *client, args [][]byte) [][]byte {
	v, ok, err := s.store.Get(string(args[1]))
	switch {
	case err != nil:
		return replyErr(c, err)
	case !ok:
		c.w.Null()
	default:
		c.w.Bulk(v)
	}
	return nil
}

// cmdSet implements SET key value [NX|XX] [EX s|PX ms|EXAT ts|PXAT ms|KEEPTTL].
// Relative TTLs are logged as absolute PXAT so replaying the AOF later doesn't
// grant keys a fresh lease on life.
func cmdSet(s *Server, c *client, args [][]byte) [][]byte {
	var opt store.SetOptions
	now := nowMs()
	for i := 3; i < len(args); i++ {
		switch flag := strings.ToUpper(string(args[i])); flag {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "KEEPTTL":
			opt.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(args) || opt.ExpireAt != 0 {
				c.w.Error(errSyntax)
				return nil
			}
			i++
			n, ok := parseInt(args[i])
			at, valid := absoluteMs(flag, n, now)
			if !ok || n <= 0 || !valid {
				c.w.Error("ERR invalid expire time in 'set' command")
				return nil
			}
			opt.ExpireAt = at
		default:
			c.w.Error(errSyntax)
			return nil
		}
	}
	if (opt.NX && opt.XX) || (opt.KeepTTL && opt.ExpireAt != 0) {
		c.w.Error(errSyntax)
		return nil
	}

	if !s.store.Set(string(args[1]), args[2], opt) {
		c.w.Null()
		return nil
	}
	c.w.SimpleString("OK")

	propagate := [][]byte{[]byte("SET"), args[1], args[2]}
	switch {
	case opt.ExpireAt != 0:
		propagate = append(propagate, []byte("PXAT"), itob(opt.ExpireAt))
	case opt.KeepTTL:
		propagate = append(propagate, []byte("KEEPTTL"))
	}
	return propagate
}

// absoluteMs converts an EX/PX/EXAT/PXAT argument to unix ms, rejecting overflow.
func absoluteMs(flag string, n, now int64) (int64, bool) {
	switch flag {
	case "EX", "EXAT":
		if n > math.MaxInt64/1000 {
			return 0, false
		}
		n *= 1000
	}
	if flag == "EX" || flag == "PX" {
		if n > math.MaxInt64-now {
			return 0, false
		}
		n += now
	}
	return n, true
}

func cmdMGet(s *Server, c *client, args [][]byte) [][]byte {
	c.w.ArrayHeader(len(args) - 1)
	for _, key := range args[1:] {
		if v, ok, err := s.store.Get(string(key)); ok && err == nil {
			c.w.Bulk(v)
		} else {
			c.w.Null() // MGET reports wrong-type keys as nil rather than failing
		}
	}
	return nil
}

func cmdMSet(s *Server, c *client, args [][]byte) [][]byte {
	if len(args)%2 != 1 {
		c.w.Error("ERR wrong number of arguments for 'mset' command")
		return nil
	}
	for i := 1; i < len(args); i += 2 {
		s.store.Set(string(args[i]), args[i+1], store.SetOptions{})
	}
	c.w.SimpleString("OK")
	return args
}

// incrBy builds INCR/DECR (fixed delta) and INCRBY/DECRBY (delta 0: read the argument).
func incrBy(delta int64) handler {
	return func(s *Server, c *client, args [][]byte) [][]byte {
		d := delta
		if d == 0 {
			n, ok := parseInt(args[2])
			if !ok {
				c.w.Error(errNotInteger)
				return nil
			}
			d = n
			if strings.EqualFold(string(args[0]), "DECRBY") {
				if n == math.MinInt64 {
					c.w.Error("ERR decrement would overflow")
					return nil
				}
				d = -n
			}
		}
		n, err := s.store.IncrBy(string(args[1]), d)
		if err != nil {
			return replyErr(c, err)
		}
		c.w.Integer(n)
		return args
	}
}

// ---- Keyspace ----

func keys(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

func cmdDel(s *Server, c *client, args [][]byte) [][]byte {
	n := s.store.Del(keys(args[1:])...)
	c.w.Integer(int64(n))
	if n == 0 {
		return nil
	}
	return args
}

func cmdExists(s *Server, c *client, args [][]byte) [][]byte {
	c.w.Integer(int64(s.store.Exists(keys(args[1:])...)))
	return nil
}

func cmdType(s *Server, c *client, args [][]byte) [][]byte {
	c.w.SimpleString(s.store.Type(string(args[1])))
	return nil
}

func cmdKeys(s *Server, c *client, args [][]byte) [][]byte {
	matched := s.store.Keys(string(args[1]))
	c.w.ArrayHeader(len(matched))
	for _, k := range matched {
		c.w.BulkString(k)
	}
	return nil
}

func cmdDBSize(s *Server, c *client, _ [][]byte) [][]byte {
	c.w.Integer(int64(s.store.Len()))
	return nil
}

func cmdFlushAll(s *Server, c *client, _ [][]byte) [][]byte {
	s.store.Flush()
	c.w.SimpleString("OK")
	return [][]byte{[]byte("FLUSHALL")}
}

// expire builds EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT. All four are logged as
// PEXPIREAT with an absolute timestamp.
func expire(unit time.Duration, absolute bool) handler {
	return func(s *Server, c *client, args [][]byte) [][]byte {
		n, ok := parseInt(args[2])
		if !ok {
			c.w.Error(errNotInteger)
			return nil
		}
		factor := int64(unit / time.Millisecond)
		if n > math.MaxInt64/factor || n < math.MinInt64/factor {
			c.w.Error("ERR invalid expire time in '" + strings.ToLower(string(args[0])) + "' command")
			return nil
		}
		at := n * factor
		if !absolute {
			at += nowMs()
		}
		if !s.store.ExpireAt(string(args[1]), at) {
			c.w.Integer(0)
			return nil
		}
		c.w.Integer(1)
		return [][]byte{[]byte("PEXPIREAT"), args[1], itob(at)}
	}
}

func ttl(unit time.Duration) handler {
	return func(s *Server, c *client, args [][]byte) [][]byte {
		ms := s.store.TTL(string(args[1]))
		if ms >= 0 && unit == time.Second {
			ms = (ms + 500) / 1000
		}
		c.w.Integer(ms)
		return nil
	}
}

func cmdPersist(s *Server, c *client, args [][]byte) [][]byte {
	if !s.store.Persist(string(args[1])) {
		c.w.Integer(0)
		return nil
	}
	c.w.Integer(1)
	return args
}

// ---- Lists ----

func push(left bool) handler {
	return func(s *Server, c *client, args [][]byte) [][]byte {
		n, err := s.store.Push(string(args[1]), left, args[2:]...)
		if err != nil {
			return replyErr(c, err)
		}
		c.w.Integer(int64(n))
		return args
	}
}

// pop builds LPOP/RPOP key [count]: a bulk reply without count, an array with it.
func pop(left bool) handler {
	return func(s *Server, c *client, args [][]byte) [][]byte {
		if len(args) > 3 {
			c.w.Error(errSyntax)
			return nil
		}
		count, withCount := int64(1), len(args) == 3
		if withCount {
			n, ok := parseInt(args[2])
			if !ok || n < 0 {
				c.w.Error("ERR value is out of range, must be positive")
				return nil
			}
			count = n
		}

		values, err := s.store.Pop(string(args[1]), left, int(min(count, math.MaxInt32)))
		switch {
		case err != nil:
			return replyErr(c, err)
		case values == nil && withCount:
			c.w.NullArray()
		case values == nil:
			c.w.Null()
		case withCount:
			writeArray(c, values)
		default:
			c.w.Bulk(values[0])
		}
		if len(values) == 0 {
			return nil
		}
		return args
	}
}

func cmdLRange(s *Server, c *client, args [][]byte) [][]byte {
	start, ok1 := parseInt(args[2])
	stop, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		c.w.Error(errNotInteger)
		return nil
	}
	values, err := s.store.Range(string(args[1]), start, stop)
	if err != nil {
		return replyErr(c, err)
	}
	writeArray(c, values)
	return nil
}

func cmdLLen(s *Server, c *client, args [][]byte) [][]byte {
	n, err := s.store.ListLen(string(args[1]))
	if err != nil {
		return replyErr(c, err)
	}
	c.w.Integer(int64(n))
	return nil
}

// ---- Hashes ----

func cmdHSet(s *Server, c *client, args [][]byte) [][]byte {
	if len(args)%2 != 0 {
		c.w.Error("ERR wrong number of arguments for 'hset' command")
		return nil
	}
	n, err := s.store.HSet(string(args[1]), args[2:]...)
	if err != nil {
		return replyErr(c, err)
	}
	c.w.Integer(int64(n))
	return args
}

func cmdHGet(s *Server, c *client, args [][]byte) [][]byte {
	v, ok, err := s.store.HGet(string(args[1]), string(args[2]))
	switch {
	case err != nil:
		return replyErr(c, err)
	case !ok:
		c.w.Null()
	default:
		c.w.Bulk(v)
	}
	return nil
}

func cmdHDel(s *Server, c *client, args [][]byte) [][]byte {
	n, err := s.store.HDel(string(args[1]), keys(args[2:])...)
	if err != nil {
		return replyErr(c, err)
	}
	c.w.Integer(int64(n))
	if n == 0 {
		return nil
	}
	return args
}

func cmdHGetAll(s *Server, c *client, args [][]byte) [][]byte {
	pairs, err := s.store.HGetAll(string(args[1]))
	if err != nil {
		return replyErr(c, err)
	}
	writeArray(c, pairs)
	return nil
}

func cmdHLen(s *Server, c *client, args [][]byte) [][]byte {
	n, err := s.store.HLen(string(args[1]))
	if err != nil {
		return replyErr(c, err)
	}
	c.w.Integer(int64(n))
	return nil
}

// ---- Transactions ----

func cmdMulti(_ *Server, c *client, _ [][]byte) [][]byte {
	if c.inMulti {
		c.w.Error("ERR MULTI calls can not be nested")
		return nil
	}
	c.inMulti = true
	c.w.SimpleString("OK")
	return nil
}

func cmdDiscard(_ *Server, c *client, _ [][]byte) [][]byte {
	if !c.inMulti {
		c.w.Error("ERR DISCARD without MULTI")
		return nil
	}
	c.inMulti, c.multiFailed, c.queued = false, false, nil
	c.w.SimpleString("OK")
	return nil
}

// cmdExec runs the queued commands while holding txLock exclusively, so no
// other client observes or interleaves with a half-applied transaction.
func cmdExec(s *Server, c *client, _ [][]byte) [][]byte {
	if !c.inMulti {
		c.w.Error("ERR EXEC without MULTI")
		return nil
	}
	queued, failed := c.queued, c.multiFailed
	c.inMulti, c.multiFailed, c.queued = false, false, nil
	if failed {
		c.w.Error("EXECABORT Transaction discarded because of previous errors.")
		return nil
	}

	s.txLock.Lock()
	defer s.txLock.Unlock()
	c.w.ArrayHeader(len(queued))
	for _, args := range queued {
		s.run(c, commands[strings.ToUpper(string(args[0]))], args)
	}
	return nil
}

// ---- Pub/Sub ----

func cmdPublish(s *Server, c *client, args [][]byte) [][]byte {
	c.w.Integer(int64(s.pubsub.publish(string(args[1]), args[2])))
	return nil
}

func subscriptionReply(c *client, kind string, channel *string, count int) {
	c.w.ArrayHeader(3)
	c.w.BulkString(kind)
	if channel == nil {
		c.w.Null()
	} else {
		c.w.BulkString(*channel)
	}
	c.w.Integer(int64(count))
}

func cmdSubscribe(s *Server, c *client, args [][]byte) [][]byte {
	for _, ch := range keys(args[1:]) {
		subscriptionReply(c, "subscribe", &ch, s.pubsub.subscribe(c, ch))
	}
	return nil
}

func cmdUnsubscribe(s *Server, c *client, args [][]byte) [][]byte {
	channels := keys(args[1:])
	if len(channels) == 0 {
		for ch := range c.subscriptions {
			channels = append(channels, ch)
		}
		if len(channels) == 0 {
			subscriptionReply(c, "unsubscribe", nil, 0)
			return nil
		}
	}
	for _, ch := range channels {
		subscriptionReply(c, "unsubscribe", &ch, s.pubsub.unsubscribe(c, ch))
	}
	return nil
}
