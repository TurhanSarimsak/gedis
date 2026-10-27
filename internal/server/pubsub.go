package server

import (
	"sync"
	"time"
)

type pubsub struct {
	mu       sync.RWMutex
	channels map[string]map[*client]struct{}
}

func newPubSub() *pubsub {
	return &pubsub{channels: make(map[string]map[*client]struct{})}
}

// subscribe returns the client's new subscription count. c.subscriptions is
// only ever touched by c's own goroutine, so it needs no lock of its own.
func (p *pubsub) subscribe(c *client, channel string) int {
	p.mu.Lock()
	subs := p.channels[channel]
	if subs == nil {
		subs = make(map[*client]struct{})
		p.channels[channel] = subs
	}
	subs[c] = struct{}{}
	p.mu.Unlock()
	c.subscriptions[channel] = struct{}{}
	return len(c.subscriptions)
}

func (p *pubsub) unsubscribe(c *client, channel string) int {
	p.mu.Lock()
	if subs := p.channels[channel]; subs != nil {
		delete(subs, c)
		if len(subs) == 0 {
			delete(p.channels, channel)
		}
	}
	p.mu.Unlock()
	delete(c.subscriptions, channel)
	return len(c.subscriptions)
}

func (p *pubsub) unsubscribeAll(c *client) {
	for channel := range c.subscriptions {
		p.unsubscribe(c, channel)
	}
}

// publish delivers msg to every subscriber and returns how many received it.
// The subscriber list is copied before delivering so p.mu is never held while
// waiting on a client's lock (which would deadlock against SUBSCRIBE).
func (p *pubsub) publish(channel string, msg []byte) int {
	p.mu.RLock()
	targets := make([]*client, 0, len(p.channels[channel]))
	for c := range p.channels[channel] {
		targets = append(targets, c)
	}
	p.mu.RUnlock()

	for _, c := range targets {
		c.deliver(channel, msg)
	}
	return len(targets)
}

func (c *client) deliver(channel string, msg []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.w.ArrayHeader(3)
	c.w.BulkString("message")
	c.w.BulkString(channel)
	c.w.Bulk(msg)
	// A subscriber that stops reading must not stall every publisher: give it
	// a deadline and drop the connection if it can't keep up.
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := c.w.Flush(); err != nil {
		c.conn.Close()
	}
	c.conn.SetWriteDeadline(time.Time{})
}
