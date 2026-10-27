// Package resp implements the Redis Serialization Protocol (RESP2).
// https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
)

const (
	MaxBulkLen  = 64 << 20 // 64 MiB per argument
	MaxArrayLen = 1 << 20  // arguments per command
	maxLineLen  = 64 << 10
)

// ProtocolError means the client sent malformed input; the connection should be closed.
type ProtocolError string

func (e ProtocolError) Error() string { return "Protocol error: " + string(e) }

// Reader parses client commands. It accepts both RESP arrays of bulk strings
// (what client libraries send) and inline commands (what you type into telnet).
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, maxLineLen)}
}

// Buffered reports bytes already received but not yet parsed. The server only
// flushes replies once this reaches zero, so a pipeline of N commands costs one
// write syscall instead of N.
func (r *Reader) Buffered() int { return r.br.Buffered() }

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, ProtocolError("line too long")
		}
		return nil, err
	}
	line = line[:len(line)-1]
	return bytes.TrimSuffix(line, []byte{'\r'}), nil
}

// ReadCommand returns the next command. io.EOF means the peer closed cleanly
// between commands; io.ErrUnexpectedEOF means it closed in the middle of one.
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			continue
		}

		if line[0] != '*' {
			fields := bytes.Fields(line)
			if len(fields) == 0 {
				continue
			}
			args := make([][]byte, len(fields))
			for i, f := range fields {
				args[i] = bytes.Clone(f) // ReadSlice's buffer is reused by the next read
			}
			return args, nil
		}

		n, err := parseLen(line[1:], MaxArrayLen)
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			continue
		}
		args := make([][]byte, n)
		for i := range args {
			if args[i], err = r.readBulk(); err != nil {
				return nil, err
			}
		}
		return args, nil
	}
}

func (r *Reader) readBulk() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, unexpected(err)
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, ProtocolError("expected '$'")
	}
	n, err := parseLen(line[1:], MaxBulkLen)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, ProtocolError("invalid bulk length")
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, unexpected(err)
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, ProtocolError("expected CRLF after bulk string")
	}
	return buf[:n:n], nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func parseLen(b []byte, max int) (int, error) {
	n, err := strconv.Atoi(string(b))
	if err != nil || n > max {
		return 0, ProtocolError("invalid length")
	}
	return n, nil
}

// Writer encodes replies into a buffer. Write errors are sticky inside
// bufio.Writer and surface from Flush, so individual calls don't return them.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 32<<10)}
}

func (w *Writer) SimpleString(s string) {
	w.bw.WriteByte('+')
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Error writes an error reply. CR/LF would terminate the reply early and let
// user-supplied text inject protocol data, so they are replaced with spaces.
func (w *Writer) Error(msg string) {
	w.bw.WriteByte('-')
	w.bw.WriteString(strings.NewReplacer("\r", " ", "\n", " ").Replace(msg))
	w.bw.WriteString("\r\n")
}

func (w *Writer) Integer(n int64) { w.prefixed(':', n) }

func (w *Writer) ArrayHeader(n int) { w.prefixed('*', int64(n)) }

func (w *Writer) Bulk(b []byte) {
	w.prefixed('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

func (w *Writer) BulkString(s string) {
	w.prefixed('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

func (w *Writer) Null() { w.bw.WriteString("$-1\r\n") }

func (w *Writer) NullArray() { w.bw.WriteString("*-1\r\n") }

func (w *Writer) Flush() error { return w.bw.Flush() }

// prefixed writes "<p><n>\r\n" using a reused scratch buffer, so replies don't allocate.
func (w *Writer) prefixed(p byte, n int64) {
	w.scratch = append(w.scratch[:0], p)
	w.scratch = strconv.AppendInt(w.scratch, n, 10)
	w.scratch = append(w.scratch, '\r', '\n')
	w.bw.Write(w.scratch)
}
