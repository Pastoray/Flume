// Command kvstore is a small CLI around the engine package: an
// interactive REPL for exercising Put/Get/Delete/Scan/Snapshot by hand, a
// concurrent benchmark mode for demonstrating throughput under
// contention, and an HTTP server mode for running the store as a
// standalone networked service.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"kvstore/engine"
	"kvstore/metrics"
	"kvstore/server"
)

func main() {
	dir := flag.String("dir", "./kvstore-data", "data directory")
	bench := flag.Bool("bench", false, "run a concurrent read/write benchmark instead of the REPL")
	benchWorkers := flag.Int("bench-workers", 8, "number of concurrent goroutines in benchmark mode")
	benchOps := flag.Int("bench-ops", 50000, "total operations per worker in benchmark mode")
	serve := flag.Bool("serve", false, "run an HTTP server instead of the REPL")
	addr := flag.String("addr", ":8080", "address to listen on in -serve mode")
	debugAddr := flag.String("debug-addr", ":6060", "address for the metrics/pprof debug endpoints (empty to disable); deliberately separate from -addr, the public API port")
	flushThreshold := flag.Int64("flush-threshold", 4*1024*1024, "MemTable flush threshold in bytes")
	l0Trigger := flag.Int("l0-compaction-trigger", 4, "number of level-0 SSTables that triggers L0->L1 compaction")
	baseLevelSize := flag.Int64("base-level-size", 2*1024*1024, "target size in bytes of level 1 before it compacts into level 2")
	levelMultiplier := flag.Int64("level-size-multiplier", 10, "growth factor between one level's target size and the next")
	compress := flag.Bool("compress", false, "enable per-value DEFLATE compression for new SSTable data")
	logJSON := flag.Bool("log-json", false, "emit structured logs as JSON instead of human-readable text (for log aggregation systems)")
	flag.Parse()

	var logHandler slog.Handler
	if *logJSON {
		logHandler = slog.NewJSONHandler(os.Stderr, nil)
	} else {
		logHandler = slog.NewTextHandler(os.Stderr, nil)
	}
	logger := slog.New(logHandler)

	e, err := engine.Open(*dir,
		engine.WithFlushThreshold(*flushThreshold),
		engine.WithL0CompactionTrigger(*l0Trigger),
		engine.WithBaseLevelSizeBytes(*baseLevelSize),
		engine.WithLevelSizeMultiplier(*levelMultiplier),
		engine.WithCompression(*compress),
		engine.WithLogger(logger),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open store at %q: %v\n", *dir, err)
		os.Exit(1)
	}
	defer func() {
		if err := e.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing store: %v\n", err)
		}
	}()

	if *debugAddr != "" {
		debugSrv := startDebugServer(*debugAddr, e.MetricsRegistry(), logger)
		fmt.Printf("debug endpoints (metrics, pprof) listening on %s\n", *debugAddr)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := debugSrv.Shutdown(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "error shutting down debug server: %v\n", err)
			}
		}()
	}

	switch {
	case *bench:
		runBenchmark(e, *benchWorkers, *benchOps)
	case *serve:
		runServer(e, *addr, logger)
	default:
		runREPL(e, *dir)
	}
}

// startDebugServer starts a metrics/pprof HTTP server on its own
// listener, deliberately separate from the public API server (-addr):
// exposing profiling and metrics on a different port (typically
// localhost-only or firewalled off in a real deployment) keeps
// operational tooling off the surface clients actually talk to.
func startDebugServer(addr string, reg *metrics.Registry, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if err := reg.Render(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	// Registered explicitly on our own mux (rather than relying on
	// net/http/pprof's package-level side effect of registering itself
	// on http.DefaultServeMux), so pprof only ever appears on this
	// dedicated debug listener.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("debug server error", "error", err)
		}
	}()
	return srv
}

// runServer starts the HTTP server and blocks until it receives an
// interrupt/termination signal, then shuts it down gracefully (the
// deferred e.Close() in main then flushes and closes the store cleanly).
func runServer(e *engine.Engine, addr string, logger *slog.Logger) {
	srv := &http.Server{Addr: addr, Handler: server.New(e, server.WithLogger(logger)).Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		fmt.Printf("kvstore HTTP server listening on %s\n", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		fmt.Println("\nshutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "error during server shutdown: %v\n", err)
		}
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		}
	}
}

func runREPL(e *engine.Engine, dir string) {
	fmt.Printf("kvstore ready at %q\n", dir)
	fmt.Println("commands: PUT <key> <value> | GET <key> | DELETE <key> | SCAN <start> <end> [limit] | SNAPSHOT | STATS | EXIT")
	fmt.Println("SCAN uses empty string \"\" for an unbounded start/end, e.g.: SCAN \"\" \"\"")

	var snap *engine.Snapshot
	defer func() {
		if snap != nil {
			snap.Close()
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if snap != nil {
			fmt.Print("(snapshot)> ")
		} else {
			fmt.Print("> ")
		}
		if !scanner.Scan() {
			fmt.Println()
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(fields[0])

		switch cmd {
		case "PUT":
			if len(fields) < 3 {
				fmt.Println("usage: PUT <key> <value>")
				continue
			}
			if err := e.Put([]byte(fields[1]), []byte(fields[2])); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "GET":
			if len(fields) < 2 {
				fmt.Println("usage: GET <key>")
				continue
			}
			var v []byte
			var err error
			if snap != nil {
				v, err = snap.Get([]byte(fields[1]))
			} else {
				v, err = e.Get([]byte(fields[1]))
			}
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Printf("%q\n", v)

		case "DELETE", "DEL":
			if len(fields) < 2 {
				fmt.Println("usage: DELETE <key>")
				continue
			}
			if err := e.Delete([]byte(fields[1])); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "SCAN":
			rest := ""
			if len(fields) >= 2 {
				rest = fields[1]
				if len(fields) == 3 {
					rest += " " + fields[2]
				}
			}
			args := strings.Fields(rest)
			var start, end []byte
			limit := 0
			if len(args) >= 1 && args[0] != `""` {
				start = []byte(strings.Trim(args[0], `"`))
			}
			if len(args) >= 2 && args[1] != `""` {
				end = []byte(strings.Trim(args[1], `"`))
			}
			if len(args) >= 3 {
				n, err := strconv.Atoi(args[2])
				if err != nil {
					fmt.Println("error: invalid limit:", err)
					continue
				}
				limit = n
			}
			runScan(e, snap, start, end, limit)

		case "SNAPSHOT":
			if snap != nil {
				fmt.Println("already inside a snapshot; use SNAPSHOT-END first")
				continue
			}
			s, err := e.Snapshot()
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			snap = s
			fmt.Println("snapshot open: GET and SCAN now read as of this point in time")

		case "SNAPSHOT-END":
			if snap == nil {
				fmt.Println("no snapshot is open")
				continue
			}
			snap.Close()
			snap = nil
			fmt.Println("snapshot closed")

		case "STATS":
			s := e.Stats()
			fmt.Printf("memtable entries: %d | immutable memtables: %d | sstables: %d | per level: %v\n",
				s.MemTableEntries, s.ImmutableMemTables, s.SSTables, s.TablesPerLevel)

		case "EXIT", "QUIT":
			return

		default:
			fmt.Printf("unknown command %q\n", fields[0])
		}
	}
}

func runScan(e *engine.Engine, snap *engine.Snapshot, start, end []byte, limit int) {
	var it *engine.ScanIterator
	var err error
	if snap != nil {
		it, err = snap.Scan(start, end)
	} else {
		it, err = e.Scan(start, end)
	}
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer it.Close()

	count := 0
	for limit == 0 || count < limit {
		key, value, ok, err := it.Next()
		if err != nil {
			fmt.Println("scan error:", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("%s = %q\n", key, value)
		count++
	}
	fmt.Printf("(%d entries)\n", count)
}

// runBenchmark hammers the store from many goroutines at once - a mix of
// writes and reads - and reports aggregate throughput and latency,
// demonstrating the engine's concurrent read/write performance under
// contention.
func runBenchmark(e *engine.Engine, workers, opsPerWorker int) {
	fmt.Printf("running benchmark: %d workers x %d ops = %d total ops\n",
		workers, opsPerWorker, workers*opsPerWorker)

	var (
		totalPuts   int64
		totalGets   int64
		totalErrors int64
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w) + time.Now().UnixNano()))
			for i := 0; i < opsPerWorker; i++ {
				key := []byte(fmt.Sprintf("bench-worker-%d-key-%d", w, rnd.Intn(1000)))
				if rnd.Intn(10) < 7 {
					// 70% writes.
					val := make([]byte, 64)
					rnd.Read(val)
					if err := e.Put(key, val); err != nil {
						atomic.AddInt64(&totalErrors, 1)
						continue
					}
					atomic.AddInt64(&totalPuts, 1)
				} else {
					// 30% reads.
					if _, err := e.Get(key); err != nil && err != engine.ErrNotFound {
						atomic.AddInt64(&totalErrors, 1)
						continue
					}
					atomic.AddInt64(&totalGets, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := totalPuts + totalGets
	fmt.Printf("\ndone in %s\n", elapsed)
	fmt.Printf("  puts:   %d\n", totalPuts)
	fmt.Printf("  gets:   %d\n", totalGets)
	fmt.Printf("  errors: %d\n", totalErrors)
	fmt.Printf("  throughput: %.0f ops/sec\n", float64(total)/elapsed.Seconds())

	s := e.Stats()
	fmt.Printf("  final state: %d memtable entries, %d immutable memtables, %d sstables, per level: %v\n",
		s.MemTableEntries, s.ImmutableMemTables, s.SSTables, s.TablesPerLevel)
}
