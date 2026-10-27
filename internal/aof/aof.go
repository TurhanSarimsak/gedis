// Package aof implements an append-only file: every write command is logged in
// RESP form and replayed on startup to rebuild the dataset.
package aof

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/TurhanSarimsak/gedis/internal/resp"
)

// Policy controls when the log is fsynced, trading durability for throughput.
type Policy int

const (
	Always   Policy = iota // fsync after every write: no data loss, slowest
	EverySec               // fsync once per second: lose at most ~1s on power loss
	Never                  // let the OS decide: fastest
)

func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "always":
		return Always, nil
	case "everysec":
		return EverySec, nil
	case "no":
		return Never, nil
	}
	return 0, fmt.Errorf("unknown fsync policy %q (want always, everysec or no)", s)
}

type AOF struct {
	mu     sync.Mutex
	file   *os.File
	w      *resp.Writer
	policy Policy
	dirty  bool
	stop   chan struct{}
	done   chan struct{}
}

func Open(path string, policy Policy) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{
		file:   f,
		w:      resp.NewWriter(f),
		policy: policy,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	if policy == Always {
		close(a.done)
	} else {
		go a.flushLoop()
	}
	return a, nil
}

// Replay calls fn for every logged command. If the process crashed halfway
// through writing the last command, that partial command is cut off the file
// (so new appends start on a clean boundary) and truncated is true.
func (a *AOF) Replay(fn func(args [][]byte)) (commands int, truncated bool, err error) {
	f, err := os.Open(a.file.Name())
	if err != nil {
		return 0, false, err
	}
	defer f.Close()

	r := resp.NewReader(f)
	var offset int64 // end of the last complete command
	for {
		args, err := r.ReadCommand()
		switch {
		case err == io.EOF:
			return commands, false, nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			// Truncate by path: on Windows an O_APPEND handle can't shrink the file.
			return commands, true, os.Truncate(a.file.Name(), offset)
		case err != nil:
			return commands, false, fmt.Errorf("corrupt AOF after %d commands (offset %d): %w", commands, offset, err)
		}
		fn(args)
		commands++
		offset += encodedLen(args)
	}
}

// encodedLen is the size of args written as a RESP array, which is exactly how
// Append writes them. That lets Replay track byte offsets through a buffered reader.
func encodedLen(args [][]byte) int64 {
	n := 1 + len(strconv.Itoa(len(args))) + 2
	for _, arg := range args {
		n += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
	}
	return int64(n)
}

func (a *AOF) Append(args [][]byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.w.ArrayHeader(len(args))
	for _, arg := range args {
		a.w.Bulk(arg)
	}
	if a.policy == Always {
		if err := a.w.Flush(); err != nil {
			return err
		}
		return a.file.Sync()
	}
	a.dirty = true
	return nil
}

func (a *AOF) flushLoop() {
	defer close(a.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-ticker.C:
			if err := a.flush(); err != nil {
				slog.Error("AOF flush failed", "err", err)
			}
		}
	}
}

func (a *AOF) flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.dirty {
		return nil
	}
	a.dirty = false
	if err := a.w.Flush(); err != nil {
		return err
	}
	if a.policy == EverySec {
		return a.file.Sync()
	}
	return nil
}

func (a *AOF) Close() error {
	if a.policy != Always {
		close(a.stop)
	}
	<-a.done
	a.mu.Lock()
	defer a.mu.Unlock()
	return errors.Join(a.w.Flush(), a.file.Sync(), a.file.Close())
}
