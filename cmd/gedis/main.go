// Command gedis runs a Redis-compatible server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TurhanSarimsak/gedis/internal/aof"
	"github.com/TurhanSarimsak/gedis/internal/server"
)

const banner = `
   ____ ____ ___  _ ____
  / __// __// _ \(_) __/   gedis %s
 / (_ // _/ / // / /\ \    a Redis-compatible server, from scratch in Go
 \___//___//____/_/___/    listening on %s
`

func main() {
	addr := flag.String("addr", ":6380", "address to listen on")
	aofPath := flag.String("aof", "", "append-only file path (empty disables persistence)")
	fsync := flag.String("appendfsync", "everysec", "AOF fsync policy: always, everysec or no")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	policy, err := aof.ParsePolicy(*fsync)
	if err != nil {
		logger.Error("invalid flag", "err", err)
		os.Exit(2)
	}

	srv, err := server.New(server.Config{Addr: *addr, AOFPath: *aofPath, AOFPolicy: policy, Logger: logger})
	if err != nil {
		logger.Error("startup failed", "err", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, banner, server.Version, *addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	select {
	case err := <-errc:
		if err != nil {
			logger.Error("server stopped", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", "err", err)
		os.Exit(1)
	}
}
