package main

import "fmt"

func main() {
	broker := NewBroker()

	publisher := NewPublisher(broker)

	for i := 1; i <= 5; i++ {
		publisher.Publish(fmt.Sprintf("Message %d", i))
	}

	sub1 := NewSubscriber("sub-1")
	sub1.Subscribe(broker)

	for i := 6; i <= 10; i++ {
		publisher.Publish(fmt.Sprintf("Message %d", i))
	}

	sub2 := NewSubscriber("sub-2")
	sub2.Subscribe(broker)

	printMessages(sub1)
	printMessages(sub2)
}

func printMessages(sub *Subscriber) {
	messages, err := sub.Read()
	if err != nil {
		fmt.Printf("%s read error: %v\n", sub.id, err)
		return
	}

	for _, message := range messages {
		fmt.Printf("%s: %s\n", sub.id, message)
	}
}