package exporter

import (
	"testing"
	"time"

	"github.com/joel-shure/lokilike/internal/domain"
)

func entry(service, level, msg string, ts time.Time, labels map[string]string) domain.LogEntry {
	return domain.LogEntry{
		Timestamp: ts,
		Service:   service,
		Level:     level,
		Message:   msg,
		Labels:    labels,
	}
}

func job(start, end time.Time, service string, labels map[string]string) *domain.ExportJob {
	return &domain.ExportJob{
		StartTime:    start,
		EndTime:      end,
		Service:      service,
		LabelFilters: labels,
	}
}

// --- matchesJob tests ---

func TestMatchesJob_InRange(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	e := entry("myapp", "info", "ok", start.Add(time.Hour), nil)

	if !matchesJob(e, job(start, end, "myapp", nil)) {
		t.Error("expected match for entry in range with matching service")
	}
}

func TestMatchesJob_BeforeRange(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	e := entry("myapp", "info", "old", start.Add(-time.Hour), nil)

	if matchesJob(e, job(start, end, "myapp", nil)) {
		t.Error("expected no match for entry before range")
	}
}

func TestMatchesJob_AfterRange(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	e := entry("myapp", "info", "future", end.Add(time.Hour), nil)

	if matchesJob(e, job(start, end, "myapp", nil)) {
		t.Error("expected no match for entry after range")
	}
}

func TestMatchesJob_ServiceMismatch(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	e := entry("other", "info", "msg", start.Add(time.Hour), nil)

	if matchesJob(e, job(start, end, "myapp", nil)) {
		t.Error("expected no match for different service")
	}
}

func TestMatchesJob_ServiceCaseInsensitive(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	e := entry("MyApp", "info", "msg", start.Add(time.Hour), nil)

	if !matchesJob(e, job(start, end, "myapp", nil)) {
		t.Error("expected case-insensitive service match")
	}
}

func TestMatchesJob_EmptyServiceMatchesAll(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	e := entry("anything", "info", "msg", start.Add(time.Hour), nil)

	if !matchesJob(e, job(start, end, "", nil)) {
		t.Error("expected empty service to match all")
	}
}

func TestMatchesJob_LabelFilterMatch(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	labels := map[string]string{"env": "prod", "region": "us-west-2"}
	e := entry("myapp", "info", "msg", start.Add(time.Hour), labels)

	filters := map[string]string{"env": "prod"}
	if !matchesJob(e, job(start, end, "", filters)) {
		t.Error("expected match when entry has required label")
	}
}

func TestMatchesJob_LabelFilterMismatch(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	labels := map[string]string{"env": "staging"}
	e := entry("myapp", "info", "msg", start.Add(time.Hour), labels)

	filters := map[string]string{"env": "prod"}
	if matchesJob(e, job(start, end, "", filters)) {
		t.Error("expected no match for wrong label value")
	}
}

func TestMatchesJob_LabelFilterMissing(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 24, 0, 0, 0, 0, time.UTC)
	e := entry("myapp", "info", "msg", start.Add(time.Hour), nil)

	filters := map[string]string{"env": "prod"}
	if matchesJob(e, job(start, end, "", filters)) {
		t.Error("expected no match when entry has no labels")
	}
}

// --- timePrefixes tests ---

func TestTimePrefixes_SingleDay(t *testing.T) {
	start := time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 23, 18, 0, 0, 0, time.UTC)

	prefixes := timePrefixes("myapp", start, end)
	if len(prefixes) != 1 {
		t.Fatalf("expected 1 prefix, got %d: %v", len(prefixes), prefixes)
	}
	if prefixes[0] != "myapp/2026/03/23/" {
		t.Errorf("unexpected prefix: %s", prefixes[0])
	}
}

func TestTimePrefixes_MultiDay(t *testing.T) {
	start := time.Date(2026, 3, 21, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)

	prefixes := timePrefixes("svc", start, end)
	if len(prefixes) != 3 {
		t.Fatalf("expected 3 prefixes, got %d: %v", len(prefixes), prefixes)
	}
	expected := []string{"svc/2026/03/21/", "svc/2026/03/22/", "svc/2026/03/23/"}
	for i, want := range expected {
		if prefixes[i] != want {
			t.Errorf("prefix[%d] = %s, want %s", i, prefixes[i], want)
		}
	}
}

func TestTimePrefixes_EmptyService(t *testing.T) {
	start := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)

	prefixes := timePrefixes("", start, end)
	if len(prefixes) != 1 {
		t.Fatalf("expected 1 prefix, got %d", len(prefixes))
	}
	if prefixes[0] != "2026/03/23/" {
		t.Errorf("unexpected prefix: %s", prefixes[0])
	}
}

func TestTimePrefixes_CrossMonth(t *testing.T) {
	start := time.Date(2026, 1, 30, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)

	prefixes := timePrefixes("svc", start, end)
	if len(prefixes) != 4 {
		t.Fatalf("expected 4 prefixes, got %d: %v", len(prefixes), prefixes)
	}
	expected := []string{
		"svc/2026/01/30/",
		"svc/2026/01/31/",
		"svc/2026/02/01/",
		"svc/2026/02/02/",
	}
	for i, want := range expected {
		if prefixes[i] != want {
			t.Errorf("prefix[%d] = %s, want %s", i, prefixes[i], want)
		}
	}
}
