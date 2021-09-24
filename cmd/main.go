package main

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"os"
)
const (
defaultBusDSN   = "amqp://guest:guest@localhost:5672/"
)

type CallEventDTO struct {
	EventID   string `json:"event_id"`
	Type      string `json:"event_type"`
	Timestamp string `json:"timestamp"`
	Data      Call   `json:"data"`
}

type Call struct {
	CallID        string `json:"call_id"`
	Queue_ID      string `json:"queue_id"`
	DialedNumber  string `json:"dialed_phone"`
	CallingNumber string `json:"calling_number"`
	CallingLevel  string `json:"calling_level"`
}

func main() {
	conn, err := amqp.Dial(defaultBusDSN)
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}
	defer amqpChannel.Close()

	queue, err := amqpChannel.QueueDeclare("calls", false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}

	err = amqpChannel.Qos(1, 0, false)
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}

	messageChannel, err := amqpChannel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}

	stopChan := make(chan bool)

	go func() {
		log.Printf("Consumer ready, PID: %d", os.Getpid())
		for d := range messageChannel {
			log.Printf("Received a message: %s", d.Body)

			var event CallEventDTO
			err := json.Unmarshal(d.Body, &event)

			if err != nil {
				log.Printf("Error decoding JSON: %s", err)
			}

			log.Println(event)

			if err := d.Ack(false); err != nil {
				log.Printf("Error acknowledging message : %s", err)
			} else {
				log.Printf("Acknowledged message")
			}

		}
	}()

	// Остановка для завершения программы
	<-stopChan

}