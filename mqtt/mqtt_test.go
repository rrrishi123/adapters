package mqtt

// Own-both-ends validation of the MQTT adapter shape: we stand up the broker,
// open a CHANNEL (subscribe), fire a CALL (publish) into the same topic, and
// assert the message arrives as a channel frame. Real MQTT 3.1.1 framing across
// a real TCP socket — no external broker, no deps. This is the "we made the
// channel, we send into it, events come back" proof the wire's websocket
// conformance does, applied to the MQTT adapter.

import (
	"strings"
	"testing"
	"time"
)

func TestMQTT_Loopback_PublishReachesSubscriber(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer br.Close()

	// CHANNEL: a subscriber holds a long-lived connection to the broker.
	sub, err := Dial(br.Addr(), "sub")
	if err != nil {
		t.Fatalf("subscriber dial: %v", err)
	}
	defer sub.Close()
	frames, err := sub.Subscribe("wire/probe")
	if err != nil {
		t.Fatalf("subscribe (open channel): %v", err)
	}

	// CALL: a publisher fires one message into the topic.
	pub, err := Dial(br.Addr(), "pub")
	if err != nil {
		t.Fatalf("publisher dial: %v", err)
	}
	defer pub.Close()
	if err := pub.Publish("wire/probe", []byte(`{"mode":"CHANNEL via MQTT","ok":true}`)); err != nil {
		t.Fatalf("publish (call): %v", err)
	}

	select {
	case m := <-frames:
		if !strings.Contains(string(m), `"ok":true`) {
			t.Fatalf("channel frame mismatch: %s", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publish -> subscribe round-trip failed: nothing arrived on the CHANNEL")
	}
}

// A publish to a topic nobody subscribed to must not reach our subscriber —
// proves the broker actually routes by topic, not just fans out blindly.
func TestMQTT_Loopback_TopicRouting(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer br.Close()

	sub, err := Dial(br.Addr(), "sub")
	if err != nil {
		t.Fatalf("subscriber dial: %v", err)
	}
	defer sub.Close()
	frames, err := sub.Subscribe("wire/a")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	pub, err := Dial(br.Addr(), "pub")
	if err != nil {
		t.Fatalf("publisher dial: %v", err)
	}
	defer pub.Close()
	if err := pub.Publish("wire/b", []byte("should-not-arrive")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-frames:
		t.Fatalf("message leaked across topics: %s", m)
	case <-time.After(300 * time.Millisecond):
		// correct: nothing arrived on the unrelated topic
	}
}
