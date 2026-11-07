package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	slog.Info("connecting")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		slog.Error("failed to connect to message broker", "err", err)
		panic(err)
	}
	defer conn.Close()
	slog.Info("connected")

	mqch, err := conn.Channel()
	if err != nil {
		slog.Error("failed to create channel", "err", err)
		panic(err)
	}

	if err := pubsub.PublishJSON(
		mqch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: true},
	); err != nil {
		panic(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch

	slog.Info("shutting down")
}
