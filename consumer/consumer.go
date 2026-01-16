package consumer

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func New(brokers []string, topic, group string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     group,
		StartOffset: kafka.FirstOffset, // если группа новая -> читать с самого начала
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
	})
	return &Consumer{reader: r}
}

func (c *Consumer) Close() error { return c.reader.Close() }

func (c *Consumer) Run(ctx context.Context) error {
	log.Println("consumer started...")
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		log.Printf("offset=%d value=%s\n", m.Offset, string(m.Value))
	}
}
