package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, input string) [][]string {
	t.Helper()
	r := NewReader(strings.NewReader(input))
	var out [][]string
	for {
		args, err := r.ReadCommand()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("ReadCommand: %v", err)
		}
		cmd := make([]string, len(args))
		for i, a := range args {
			cmd[i] = string(a)
		}
		out = append(out, cmd)
	}
}

func TestReadPipelinedAndInline(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$12\r\nhello\r\nworld\r\n" + // binary-safe: value contains CRLF
		"*1\r\n$4\r\nPING\r\n" +
		"\r\n" + // blank lines are ignored
		"GET  key\n" // inline, LF only, extra spaces

	got := readAll(t, input)
	want := [][]string{{"SET", "key", "hello\r\nworld"}, {"PING"}, {"GET", "key"}}
	if len(got) != len(want) {
		t.Fatalf("got %d commands, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if strings.Join(got[i], "|") != strings.Join(want[i], "|") {
			t.Errorf("command %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestReadErrors(t *testing.T) {
	cases := map[string]error{
		"*2\r\n$3\r\nGET\r\n":                  io.ErrUnexpectedEOF, // second argument missing
		"*1\r\n$3\r\nGE":                       io.ErrUnexpectedEOF,
		"*1\r\n:3\r\n":                         ProtocolError(""),
		"*1\r\n$3\r\nGETX\r\n":                 ProtocolError(""),
		"*x\r\n":                               ProtocolError(""),
		"*1\r\n$999999999999\r\n":              ProtocolError(""),
		"*" + strings.Repeat("9", 20) + "\r\n": ProtocolError(""),
	}
	for input, want := range cases {
		_, err := NewReader(strings.NewReader(input)).ReadCommand()
		var perr ProtocolError
		switch {
		case errors.As(want, &perr):
			if !errors.As(err, &perr) {
				t.Errorf("%q: got %v, want ProtocolError", input, err)
			}
		case !errors.Is(err, want):
			t.Errorf("%q: got %v, want %v", input, err, want)
		}
	}
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SimpleString("OK")
	w.Error("ERR bad\r\ninjected")
	w.Integer(-42)
	w.ArrayHeader(2)
	w.Bulk([]byte("a\r\nb"))
	w.Null()
	w.NullArray()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n-ERR bad  injected\r\n:-42\r\n*2\r\n$4\r\na\r\nb\r\n$-1\r\n*-1\r\n"
	if buf.String() != want {
		t.Fatalf("got %q\nwant %q", buf.String(), want)
	}
}

// FuzzReadCommand feeds random bytes to the parser: it must never panic or
// allocate unbounded memory, only return commands or errors.
func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("*-1\r\n*1\r\n$-1\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for range 100 {
			if _, err := r.ReadCommand(); err != nil {
				return
			}
		}
	})
}
