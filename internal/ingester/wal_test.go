package ingester

import (
	"testing"
	"time"

	"github.com/joel-shure/sigyn/internal/domain"
)

func TestWAL_AppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	entries := []domain.LogEntry{
		{Timestamp: time.Now(), Service: "svc", Level: "info", Message: "one"},
		{Timestamp: time.Now(), Service: "svc", Level: "error", Message: "two"},
	}

	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	wal.Close()

	// Reopen and recover.
	wal2, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL (reopen): %v", err)
	}
	defer wal2.Close()

	recovered, err := wal2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(recovered) != 2 {
		t.Fatalf("expected 2 recovered entries, got %d", len(recovered))
	}
	if recovered[0].Message != "one" || recovered[1].Message != "two" {
		t.Errorf("unexpected recovered messages: %v", recovered)
	}
}

func TestWAL_ResetTruncates(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	wal.Append(domain.LogEntry{Timestamp: time.Now(), Message: "before"})

	if err := wal.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("expected 0 entries after reset, got %d", len(recovered))
	}
}

func TestWAL_AppendAfterReset(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	wal.Append(domain.LogEntry{Timestamp: time.Now(), Message: "old"})
	wal.Reset()
	wal.Append(domain.LogEntry{Timestamp: time.Now(), Message: "new"})

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(recovered))
	}
	if recovered[0].Message != "new" {
		t.Errorf("message = %q, want new", recovered[0].Message)
	}
}

func TestWAL_EmptyRecover(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("expected 0 entries from fresh WAL, got %d", len(recovered))
	}
}
