package ingester

import (
	"sync"

	"github.com/google/uuid"
	"github.com/joel-shure/sigyn/internal/domain"
)

// Broker fans out ingested log entries to live-tail subscribers.
type Broker struct {
	mu   sync.RWMutex
	subs map[string]*Subscription
}

// Subscription is a live-tail subscriber.
type Subscription struct {
	ID     string
	Ch     chan domain.LogEntry
	Filter func(domain.LogEntry) bool
}

// NewBroker creates a broker with no subscribers.
func NewBroker() *Broker {
	return &Broker{subs: make(map[string]*Subscription)}
}

// Subscribe registers a new subscriber. The returned channel receives
// entries that pass the filter. Buffer size is 256; slow consumers drop entries.
func (b *Broker) Subscribe(filter func(domain.LogEntry) bool) *Subscription {
	sub := &Subscription{
		ID:     uuid.NewString(),
		Ch:     make(chan domain.LogEntry, 256),
		Filter: filter,
	}
	b.mu.Lock()
	b.subs[sub.ID] = sub
	b.mu.Unlock()
	return sub
}

// Unsubscribe removes a subscriber and closes its channel.
func (b *Broker) Unsubscribe(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[id]; ok {
		delete(b.subs, id)
	}
}

// Publish sends an entry to all subscribers whose filter matches.
// Non-blocking: entries are dropped if a subscriber's channel is full.
func (b *Broker) Publish(entry domain.LogEntry) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sub := range b.subs {
		if sub.Filter == nil || sub.Filter(entry) {
			select {
			case sub.Ch <- entry:
			default:
				// Subscriber too slow, drop entry.
			}
		}
	}
}
