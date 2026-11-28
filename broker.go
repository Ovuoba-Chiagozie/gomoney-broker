package main

import (
	"errors"
	"sync"
)

var ErrNotSubscribed = errors.New("subscriber is not subscribed to a broker")

type Broker struct {
	mu          sync.Mutex
	messages    []string
	subscribers map[string]*Subscriber
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string]*Subscriber),
	}
}

type Publisher struct {
	broker *Broker
}

func NewPublisher(broker *Broker) *Publisher {
	return &Publisher{broker: broker}
}

func (p *Publisher) Publish(text string) {
	p.broker.mu.Lock()
	defer p.broker.mu.Unlock()

	p.broker.messages = append(p.broker.messages, text)
}

type Subscriber struct {
	id       string
	position int
	broker   *Broker
}

func NewSubscriber(id string) *Subscriber {
	return &Subscriber{id: id}
}

// Subscribe is a no-op if the id is already subscribed.
func (s *Subscriber) Subscribe(broker *Broker) {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.subscribers[s.id] != nil {
		return
	}

	broker.subscribers[s.id] = s
	s.broker = broker
}

// Read returns all the unread messages and moves the position to the end.
func (s *Subscriber) Read() ([]string, error) {
	if s.broker == nil {
		return nil, ErrNotSubscribed
	}

	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()

	unread := make([]string, len(s.broker.messages)-s.position)
	copy(unread, s.broker.messages[s.position:])
	s.position = len(s.broker.messages)

	return unread, nil
}
