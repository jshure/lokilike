package ingester

import (
	"testing"
	"time"

	"github.com/joel-shure/sigyn/internal/domain"
)

func TestBroker_PublishToSubscriber(t *testing.T) {
	b := NewBroker()
	sub := b.Subscribe(nil) // no filter = receive all
	defer b.Unsubscribe(sub.ID)

	entry := domain.LogEntry{
		Timestamp: time.Now(), Service: "svc", Level: "info", Message: "hello",
	}
	b.Publish(entry)

	select {
	case got := <-sub.Ch:
		if got.Message != "hello" {
			t.Errorf("message = %q, want %q", got.Message, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for entry")
	}
}

func TestBroker_FilteredSubscriber(t *testing.T) {
	b := NewBroker()
	sub := b.Subscribe(func(e domain.LogEntry) bool {
		return e.Level == "error"
	})
	defer b.Unsubscribe(sub.ID)

	b.Publish(domain.LogEntry{Level: "info", Message: "dropped"})
	b.Publish(domain.LogEntry{Level: "error", Message: "kept"})

	select {
	case got := <-sub.Ch:
		if got.Message != "kept" {
			t.Errorf("message = %q, want %q", got.Message, "kept")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for filtered entry")
	}

	// Channel should be empty (info was filtered out).
	select {
	case extra := <-sub.Ch:
		t.Fatalf("unexpected extra entry: %+v", extra)
	default:
		// good
	}
}

func TestBroker_MultipleSubscribers(t *testing.T) {
	b := NewBroker()
	sub1 := b.Subscribe(nil)
	sub2 := b.Subscribe(nil)
	defer b.Unsubscribe(sub1.ID)
	defer b.Unsubscribe(sub2.ID)

	b.Publish(domain.LogEntry{Message: "fan-out"})

	for _, sub := range []*Subscription{sub1, sub2} {
		select {
		case got := <-sub.Ch:
			if got.Message != "fan-out" {
				t.Errorf("sub %s: message = %q, want fan-out", sub.ID, got.Message)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub %s: timeout", sub.ID)
		}
	}
}

func TestBroker_UnsubscribeStopsDelivery(t *testing.T) {
	b := NewBroker()
	sub := b.Subscribe(nil)

	b.Unsubscribe(sub.ID)
	b.Publish(domain.LogEntry{Message: "after-unsub"})

	select {
	case <-sub.Ch:
		t.Fatal("should not receive after unsubscribe")
	case <-time.After(50 * time.Millisecond):
		// good
	}
}

func TestBroker_SlowConsumerDrops(t *testing.T) {
	b := NewBroker()
	sub := b.Subscribe(nil)
	defer b.Unsubscribe(sub.ID)

	// Fill the channel buffer (256) and then some.
	for i := 0; i < 300; i++ {
		b.Publish(domain.LogEntry{Message: "flood"})
	}

	// Should not panic or block. Drain what we can.
	count := 0
	for {
		select {
		case <-sub.Ch:
			count++
		default:
			goto done
		}
	}
done:
	if count > 256 {
		t.Fatalf("expected at most 256 entries (buffer size), got %d", count)
	}
}
