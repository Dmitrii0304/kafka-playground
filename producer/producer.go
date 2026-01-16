package producer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	broker string
	topic  string
}

type Msg struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
	Time string `json:"time"`
}

func New(broker, topic string) *Producer {
	return &Producer{broker: broker, topic: topic}
}

func (p *Producer) CreateTopic(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", p.broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             p.topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}

func (p *Producer) SendBatch(ctx context.Context, n int) error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(p.broker),
		Topic:    p.topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	msgs := make([]kafka.Message, 0, n)
	for i := 1; i <= n; i++ {
		body, err := json.Marshal(Msg{
			ID:   i,
			Text: "hello kafka",
			Time: time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{Value: body})
	}

	log.Printf("sending %d messages to %q\n", n, p.topic)
	return w.WriteMessages(ctx, msgs...)
}
