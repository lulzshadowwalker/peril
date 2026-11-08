package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Handler func(ch *amqp.Channel)

var commands map[string]Handler = map[string]Handler{
	"pause": func(ch *amqp.Channel) {
		fmt.Println("sending a pause message")
		publish(ch, routing.PauseKey, routing.PlayingState{IsPaused: true})
	},

	"resume": func(ch *amqp.Channel) {

		fmt.Println("sending a resume message")
		publish(ch, routing.PauseKey, routing.PlayingState{IsPaused: false})
	},

	"quit": func(ch *amqp.Channel) {

		fmt.Println("exiting")
	},
}

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

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		cmd := strings.ToLower(strings.TrimSpace(words[0]))
		if cmd == "exit"  {
			break
		}

		if fn, ok := commands[cmd]; ok {
			fn(mqch)
			continue
		}

		fmt.Println("what ..")

	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch

	slog.Info("shutting down")
}

func publish(ch *amqp.Channel, key string, val any) {
	if err := pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		key,
		val,
	); err != nil {
		panic(err)
	}

}
