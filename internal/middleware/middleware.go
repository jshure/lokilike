package middleware

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"

	"github.com/joel-shure/lokilike/internal/metrics"
)

// Chain applies middlewares in order. The first middleware is the outermost wrapper.
func Chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// AccessLog emits a structured JSON log line for every HTTP request and
// records prometheus HTTP metrics.
func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		dur := time.Since(start)

		slog.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration_ms", dur.Milliseconds(),
			"bytes", rw.bytes,
			"remote", r.RemoteAddr,
		)
		metrics.HTTPRequests.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rw.status)).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, r.URL.Path).Observe(dur.Seconds())
	})
}

// BodyLimit restricts the maximum request body size.
func BodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}

// APIKeyAuth rejects requests without a valid X-API-Key header.
// Health and metrics endpoints are exempt.
func APIKeyAuth(keys []string) func(http.Handler) http.Handler {
	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}
			key := r.Header.Get("X-API-Key")
			if _, ok := keySet[key]; !ok {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RateLimit applies a per-server token-bucket rate limiter.
// rps is requests-per-second; burst is the maximum burst size.
func RateLimit(rps float64, burst int) func(http.Handler) http.Handler {
	limiter := rate.NewLimiter(rate.Limit(rps), burst)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.Allow() {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// statusWriter wraps ResponseWriter to capture the status code and bytes written.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}
