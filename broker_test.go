package main

import (
	"fmt"
	"slices"
	"sync"
	"testing"
)

func assertMessages(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func readOK(t *testing.T, s *Subscriber) []string {
	t.Helper()
	got, err := s.Read()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return got
}

func TestPublisherCanPublishMultipleMessages(t *testing.T) {
	broker := NewBroker()
	publisher := NewPublisher(broker)

	publisher.Publish("one")
	publisher.Publish("two")
	publisher.Publish("three")

	assertMessages(t, broker.messages, []string{"one", "two", "three"})
}

func TestSubscribeTwiceIsNoOp(t *testing.T) {
	broker := NewBroker()
	publisher := NewPublisher(broker)
	sub := NewSubscriber("sub-1")

	sub.Subscribe(broker)
	publisher.Publish("one")
	readOK(t, sub)
	sub.Subscribe(broker)

	publisher.Publish("two")
	assertMessages(t, readOK(t, sub), []string{"two"})
	if len(broker.subscribers) != 1 {
		t.Errorf("got %d subscribers want 1", len(broker.subscribers))
	}
}

func TestMessagesAreReadInPublishOrder(t *testing.T) {
	broker := NewBroker()
	publisher := NewPublisher(broker)
	sub := NewSubscriber("sub-1")
	sub.Subscribe(broker)

	publisher.Publish("one")
	publisher.Publish("two")
	publisher.Publish("three")

	assertMessages(t, readOK(t, sub), []string{"one", "two", "three"})
}

func TestEachMessageIsConsumedOnlyOnce(t *testing.T) {
	broker := NewBroker()
	publisher := NewPublisher(broker)
	sub := NewSubscriber("sub-1")
	sub.Subscribe(broker)

	publisher.Publish("one")
	assertMessages(t, readOK(t, sub), []string{"one"})
	assertMessages(t, readOK(t, sub), []string{})

	publisher.Publish("two")
	assertMessages(t, readOK(t, sub), []string{"two"})
}

func TestSubscribersHaveIndependentPositions(t *testing.T) {
	broker := NewBroker()
	publisher := NewPublisher(broker)
	sub1 := NewSubscriber("sub-1")
	sub2 := NewSubscriber("sub-2")
	sub1.Subscribe(broker)
	sub2.Subscribe(broker)

	publisher.Publish("one")
	publisher.Publish("two")

	assertMessages(t, readOK(t, sub1), []string{"one", "two"})
	assertMessages(t, readOK(t, sub2), []string{"one", "two"})
}

func TestLateSubscriberReadsFullHistory(t *testing.T) {
	broker := NewBroker()
	publisher := NewPublisher(broker)
	publisher.Publish("one")
	publisher.Publish("two")

	late := NewSubscriber("late")
	late.Subscribe(broker)

	assertMessages(t, readOK(t, late), []string{"one", "two"})
}

func TestReadBeforeSubscribeReturnsError(t *testing.T) {
	sub := NewSubscriber("sub-1")

	if _, err := sub.Read(); err != ErrNotSubscribed {
		t.Errorf("got error %v want %v", err, ErrNotSubscribed)
	}
}

func TestConcurrentPublishAndRead(t *testing.T) {
	broker := NewBroker()
	publisher := NewPublisher(broker)
	sub := NewSubscriber("sub-1")
	sub.Subscribe(broker)

	const total = 1000
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			publisher.Publish(fmt.Sprintf("msg-%d", i))
		}(i)
	}

	var got []string
	for len(got) < total {
		got = append(got, readOK(t, sub)...)
	}
	wg.Wait()

	if len(got) != total {
		t.Errorf("got %d messages want %d", len(got), total)
	}
}