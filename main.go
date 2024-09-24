package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Message struct for messages being published
type Message struct {
	Topic   string      `json:"topic"`
	Payload interface{} `json:"payload"`
}

// Subscriber struct represents a subscriber to a topic
type Subscriber struct {
	Channel     chan interface{}
	Unsubscribe chan bool
}

// Broker struct manages subscriptions and publishing
type Broker struct {
	subscribers map[string][]*Subscriber
	mutex       sync.Mutex
	messages    []Message // Store messages for persistence
}

// NewBroker returns a new instance of Broker
func NewBroker() *Broker {
	broker := &Broker{
		subscribers: make(map[string][]*Subscriber),
	}
	broker.loadMessages() // Load messages from file on startup
	return broker
}

// Subscribe adds a new subscriber to a topic
func (b *Broker) Subscribe(topic string) *Subscriber {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	subscriber := &Subscriber{
		Channel:     make(chan interface{}, 10), // Buffered channel to prevent blocking
		Unsubscribe: make(chan bool),
	}
	b.subscribers[topic] = append(b.subscribers[topic], subscriber)

	// Send previously stored messages to the new subscriber
	for _, msg := range b.messages {
		if msg.Topic == topic {
			select {
			case subscriber.Channel <- msg.Payload:
			default: // Avoid blocking if the channel is full
			}
		}
	}

	return subscriber
}

// Unsubscribe removes a subscriber from a topic
func (b *Broker) Unsubscribe(topic string, subscriber *Subscriber) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if subscribers, found := b.subscribers[topic]; found {
		for i, sub := range subscribers {
			if sub == subscriber {
				close(sub.Channel)
				b.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
				return
			}
		}
	}
}

// Publish sends a message to all subscribers of a topic and saves it
func (b *Broker) Publish(topic string, payload interface{}) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	message := Message{Topic: topic, Payload: payload}
	b.messages = append(b.messages, message) // Save message for persistence
	b.saveMessages()                         // Save to file

	if subscribers, found := b.subscribers[topic]; found {
		for _, sub := range subscribers {
			select {
			case sub.Channel <- payload:
			case <-time.After(time.Second):
				fmt.Printf("Subscriber is slow, unsubscribing from topic %s\n", topic)
				b.Unsubscribe(topic, sub)
			}
		}
	}
}

// DeleteTopic removes all subscribers and the topic itself from the broker
func (b *Broker) DeleteTopic(topic string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// Check if the topic exists
	if subscribers, found := b.subscribers[topic]; found {
		// Close all subscribers' channels
		for _, sub := range subscribers {
			close(sub.Channel) // Ensure the channel is cleaned up
		}
		// Remove the topic from the map
		delete(b.subscribers, topic) // Delete the topic
		fmt.Printf("Deleted topic %s along with its subscribers.\n", topic)
	} else {
		fmt.Printf("Topic %s not found.\n", topic)
	}
}

// Save messages to a JSON file
func (b *Broker) saveMessages() {
	file, err := os.Create("messages.json")
	if err != nil {
		fmt.Println("Error creating messages file:", err)
		return
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(b.messages); err != nil {
		fmt.Println("Error encoding messages:", err)
	}
}

// Load messages from a JSON file
func (b *Broker) loadMessages() {
	file, err := os.Open("messages.json")
	if err != nil {
		if os.IsNotExist(err) {
			return // No previous messages
		}
		fmt.Println("Error opening messages file:", err)
		return
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&b.messages); err != nil {
		fmt.Println("Error decoding messages:", err)
	}
}

// Main function to run the broker
func main() {
	broker := NewBroker()
	subscriber := broker.Subscribe("GSoC")

	go func() {
		for {
			select {
			case msg, ok := <-subscriber.Channel:
				if !ok {
					fmt.Println("Channel closed")
					return
				}
				fmt.Printf("Received: %s\n", msg)
			case <-subscriber.Unsubscribe:
				fmt.Println("Unsubscribed")
				return
			}
		}
	}()

	broker.Publish("GSoC", "Hello GSoC!")

	time.Sleep(2 * time.Second)

	broker.Unsubscribe("GSoC", subscriber)
	broker.Publish("arpit", "ho gya")
	time.Sleep(time.Second)

	// Delete the topic "GSoC"
	broker.DeleteTopic("GSoC")
	//broker.Publish("GSoC", "This should not be received") 
	// You can delete other topics similarly
	broker.DeleteTopic("arpit")
}
