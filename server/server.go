// Package server exposes an *engine.Engine over a small HTTP/JSON REST
// API, built entirely on the standard library (net/http's Go 1.22+
// method-and-wildcard routing), so the store can run as a standalone
// networked service driven by curl or any HTTP client - no third-party
// web framework or RPC library involved.
//
// Endpoints:
//
//	PUT    /kv/{key}   body = raw value bytes  -> 204 on success
//	GET    /kv/{key}                            -> 200 + raw value bytes, 404 if absent
//	DELETE /kv/{key}                            -> 204 on success
//	GET    /scan?start=&end=&limit=             -> 200 + JSON array of {"key","value"} (base64-encoded)
//	GET    /stats                                -> 200 + JSON engine.Stats
//	GET    /healthz                              -> 200 "ok"
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"kvstore/engine"
)

// maxRequestBody bounds how much of a PUT request body will be read,
// guarding against an unbounded allocation from a malicious or mistaken
// client.
const maxRequestBody = 64 << 20 // 64 MiB

// Server wraps an *engine.Engine with an HTTP handler.
type Server struct {
	e      *engine.Engine
	mux    *http.ServeMux
	logger *slog.Logger
}

// Option configures a Server at construction time.
type Option func(*Server)

// WithLogger sets the structured logger used for per-request access
// logging. Defaults to slog.Default() if not set.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) { s.logger = l }
}

// New builds a Server around an already-open Engine. The Engine's
// lifecycle (in particular, calling Close) remains the caller's
// responsibility.
func New(e *engine.Engine, opts ...Option) *Server {
	s := &Server{e: e, mux: http.NewServeMux()}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	s.routes()
	return s
}

// Handler returns the http.Handler to pass to http.Server, or to
// http.ListenAndServe directly.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("PUT /kv/{key}", s.withLogging(s.handlePut))
	s.mux.HandleFunc("GET /kv/{key}", s.withLogging(s.handleGet))
	s.mux.HandleFunc("DELETE /kv/{key}", s.withLogging(s.handleDelete))
	s.mux.HandleFunc("GET /scan", s.withLogging(s.handleScan))
	s.mux.HandleFunc("GET /stats", s.withLogging(s.handleStats))
	s.mux.HandleFunc("GET /healthz", s.withLogging(s.handleHealthz))
}

// statusRecorder wraps http.ResponseWriter to capture the status code
// actually written, so withLogging can log it (ResponseWriter itself
// doesn't expose what was written after the fact).
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// withLogging wraps a handler with a structured access log line: method,
// path, resulting status code, and duration. Access logging is a
// separate concern from the engine's own operational metrics/logs - this
// is specifically about what the HTTP surface served, not what the
// storage engine did underneath it.
func (s *Server) withLogging(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		s.logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		http.Error(w, fmt.Sprintf("reading request body: %v", err), http.StatusBadRequest)
		return
	}
	if len(value) > maxRequestBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := s.e.Put([]byte(key), value); err != nil {
		s.writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, err := s.e.Get([]byte(key))
	if err != nil {
		s.writeEngineError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(value)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.e.Delete([]byte(key)); err != nil {
		s.writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// scanEntry's []byte fields are automatically base64-encoded by
// encoding/json, keeping the endpoint binary-safe for arbitrary key/value
// bytes without any custom encoding logic.
type scanEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var start, end []byte
	if v := q.Get("start"); v != "" {
		start = []byte(v)
	}
	if v := q.Get("end"); v != "" {
		end = []byte(v)
	}
	limit := 0 // 0 means unlimited
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = n
	}

	it, err := s.e.Scan(start, end)
	if err != nil {
		s.writeEngineError(w, err)
		return
	}
	defer it.Close()

	results := []scanEntry{}
	for limit == 0 || len(results) < limit {
		key, value, ok, err := it.Next()
		if err != nil {
			http.Error(w, fmt.Sprintf("scan error: %v", err), http.StatusInternalServerError)
			return
		}
		if !ok {
			break
		}
		results = append(results, scanEntry{Key: key, Value: value})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(results); err != nil {
		s.logger.Error("encoding scan response", "error", err)
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.e.Stats()); err != nil {
		s.logger.Error("encoding stats response", "error", err)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) writeEngineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, engine.ErrEmptyKey):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, engine.ErrClosed):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		s.logger.Error("internal server error", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
