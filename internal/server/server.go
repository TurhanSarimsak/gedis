// Package server accepts TCP connections and executes Redis commands against the store.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TurhanSarimsak/gedis/internal/aof"
	"github.com/TurhanSarimsak/gedis/internal/resp"
	"github.com/TurhanSarimsak/gedis/internal/store"
)

const Version = "0.1.0"

type Config struct {
	Addr      string
	AOFPath   string // empty disables persistence
	AOFPolicy aof.Policy
	Logger    *slog.Logger
}

type Server struct {
	cfg    Config
	log    *slog.Logger
	store  *store.Store
	aof    *aof.AOF
	pubsub *pubsub

	// Regular commands hold txLock for reading; EXEC holds it for writing, so a
	// transaction runs atomically across all shards.
	txLock sync.RWMutex
	// With the AOF on, write commands are serialised so the log records them in
	// exactly the order they were applied. Otherwise two concurrent SETs on one
	// key could be applied A,B but logged B,A, and a restart would resurrect A.
	aofOrder sync.Mutex

	mu      sync.Mutex
	ln      net.Listener
	clients map[*client]struct{}
	closing bool
	wg      sync.WaitGroup

	shutdownOnce sync.Once
	shutdownErr  error
	stopExpiry   chan struct{}

	startedAt     time.Time
	totalCommands atomic.Int64
	totalConns    atomic.Int64
}

type client struct {
	conn net.Conn
	r    *resp.Reader
	w    *resp.Writer
	// mu guards w: PUBLISH writes to subscribers from other connections' goroutines.
	mu sync.Mutex

	subscriptions map[string]struct{}
	inMulti       bool
	multiFailed   bool
	queued        [][][]byte
}

func newClient(conn net.Conn) *client {
	return &client{
		conn:          conn,
		r:             resp.NewReader(conn),
		w:             resp.NewWriter(conn),
		subscriptions: make(map[string]struct{}),
	}
}

func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Server{
		cfg:        cfg,
		log:        cfg.Logger,
		store:      store.New(),
		pubsub:     newPubSub(),
		clients:    make(map[*client]struct{}),
		stopExpiry: make(chan struct{}),
		startedAt:  time.Now(),
	}

	if cfg.AOFPath != "" {
		log, err := aof.Open(cfg.AOFPath, cfg.AOFPolicy)
		if err != nil {
			return nil, err
		}
		// Replay through the normal command path; s.aof is still nil, so
		// nothing is logged twice.
		replay := &client{w: resp.NewWriter(io.Discard), subscriptions: map[string]struct{}{}}
		start := time.Now()
		n, truncated, err := log.Replay(func(args [][]byte) {
			s.execute(replay, strings.ToUpper(string(args[0])), args)
		})
		if err != nil {
			log.Close()
			return nil, fmt.Errorf("loading AOF: %w", err)
		}
		if truncated {
			s.log.Warn("AOF ended with a partial command (crash during write?); truncated it")
		}
		s.log.Info("AOF loaded", "commands", n, "keys", s.store.Len(), "took", time.Since(start).Round(time.Millisecond))
		s.aof = log
	}

	go s.expiryLoop()
	return s, nil
}

func (s *Server) expiryLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopExpiry:
			return
		case <-ticker.C:
			s.store.ExpireCycle()
		}
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		ln.Close()
		return nil
	}
	s.ln = ln
	s.mu.Unlock()
	s.log.Info("ready to accept connections", "addr", ln.Addr().String(), "version", Version)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		c := newClient(conn)
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			conn.Close()
			return nil
		}
		s.clients[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		s.totalConns.Add(1)
		go s.handle(c)
	}
}

// Shutdown stops accepting connections, disconnects clients, waits for their
// in-flight commands and flushes the AOF. It is safe to call more than once.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		if s.ln != nil {
			s.ln.Close()
		}
		for c := range s.clients {
			c.conn.Close()
		}
		s.mu.Unlock()
		close(s.stopExpiry)

		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.shutdownErr = ctx.Err()
		}
		if s.aof != nil {
			s.shutdownErr = errors.Join(s.shutdownErr, s.aof.Close())
		}
	})
	return s.shutdownErr
}

func (s *Server) handle(c *client) {
	defer func() {
		s.pubsub.unsubscribeAll(c)
		c.conn.Close()
		s.mu.Lock()
		delete(s.clients, c)
		s.mu.Unlock()
		s.wg.Done()
	}()

	for {
		args, err := c.r.ReadCommand()
		if err != nil {
			var perr resp.ProtocolError
			if errors.As(err, &perr) {
				c.mu.Lock()
				c.w.Error("ERR " + perr.Error())
				c.w.Flush()
				c.mu.Unlock()
			}
			return
		}

		c.mu.Lock()
		quit := s.dispatch(c, args)
		// Only flush once the pipeline is drained: one syscall for many replies.
		var flushErr error
		if quit || c.r.Buffered() == 0 {
			flushErr = c.w.Flush()
		}
		c.mu.Unlock()
		if quit || flushErr != nil {
			return
		}
	}
}

var (
	// Commands allowed while a connection is subscribed to channels (RESP2 rules).
	subscribeContext = map[string]bool{"SUBSCRIBE": true, "UNSUBSCRIBE": true, "PING": true, "QUIT": true}
	// Commands that run immediately even inside MULTI.
	txControl = map[string]bool{"MULTI": true, "EXEC": true, "DISCARD": true}
)

// dispatch handles connection-level state (QUIT, pub/sub mode, MULTI queueing)
// and reports whether the connection should be closed.
func (s *Server) dispatch(c *client, args [][]byte) (quit bool) {
	s.totalCommands.Add(1)
	name := strings.ToUpper(string(args[0]))

	switch {
	case name == "QUIT":
		c.w.SimpleString("OK")
		return true
	case len(c.subscriptions) > 0 && !subscribeContext[name]:
		c.w.Error(fmt.Sprintf("ERR Can't execute '%s': only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are allowed in this context", strings.ToLower(name)))
	case c.inMulti && !txControl[name]:
		if _, ok := s.validate(c, name, args); ok {
			c.queued = append(c.queued, args)
			c.w.SimpleString("QUEUED")
		} else {
			c.multiFailed = true // Redis aborts the whole transaction on EXEC
		}
	default:
		s.execute(c, name, args)
	}
	return false
}

// validate looks up a command and checks its arity, replying with an error if either fails.
func (s *Server) validate(c *client, name string, args [][]byte) (command, bool) {
	cmd, ok := commands[name]
	if !ok {
		c.w.Error(fmt.Sprintf("ERR unknown command '%s'", truncate(string(args[0]), 64)))
		return cmd, false
	}
	if !cmd.arityOK(len(args)) {
		c.w.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
		return cmd, false
	}
	return cmd, true
}

func (s *Server) execute(c *client, name string, args [][]byte) {
	cmd, ok := s.validate(c, name, args)
	if !ok {
		return
	}
	if !cmd.noLock {
		s.txLock.RLock()
		defer s.txLock.RUnlock()
	}
	if cmd.write && s.aof != nil {
		s.aofOrder.Lock()
		defer s.aofOrder.Unlock()
	}
	s.run(c, cmd, args)
}

// run executes a validated command and logs whatever it asks to propagate.
func (s *Server) run(c *client, cmd command, args [][]byte) {
	propagate := cmd.handler(s, c, args)
	if propagate != nil && s.aof != nil {
		if err := s.aof.Append(propagate); err != nil {
			s.log.Error("AOF append failed", "err", err)
		}
	}
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
