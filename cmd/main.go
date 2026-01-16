package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"kafka-playground/config"
	"kafka-playground/consumer"
	"kafka-playground/producer"
)

func main() {
	mode := flag.String("mode", "producer", "producer|consumer")
	n := flag.Int("n", 150, "batch size (100-200)")
	flag.Parse()

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch strings.ToLower(*mode) {
	case "producer":
		if *n < 100 || *n > 200 {
			log.Printf("warning: n=%d is outside 100-200 (task says 100-200)\n", *n)
		}

		p := producer.New(cfg.Broker, cfg.Topic)

		tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := p.CreateTopic(tctx); err != nil {
			log.Printf("create topic: %v (ignore if already exists)\n", err)
		}

		sctx, cancel2 := context.WithTimeout(ctx, 30*time.Second)
		defer cancel2()
		if err := p.SendBatch(sctx, *n); err != nil {
			log.Fatalf("send batch: %v", err)
		}

		log.Println("producer done")

	case "consumer":
		c := consumer.New([]string{cfg.Broker}, cfg.Topic, cfg.Group)
		defer c.Close()

		if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("consumer: %v", err)
		}

	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}
