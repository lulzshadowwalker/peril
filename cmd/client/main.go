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

type Args struct {
	State *gamelogic.GameState
	Words []string
}

type Handler func(ch *amqp.Channel, args Args)

var commands map[string]Handler = map[string]Handler{
	"spawn": func(ch *amqp.Channel, args Args) {
		if err := args.State.CommandSpawn(args.Words); err != nil {
			slog.Error("failed to spawn", "err", err, "words", args.Words)
			return
		}
	},

	"move": func(ch *amqp.Channel, args Args) {
		if _, err := args.State.CommandMove(args.Words); err != nil {
			slog.Error("failed to move", "err", err, "words", args.Words)
			return
		}
	},

	"status": func(ch *amqp.Channel, args Args) {
		args.State.CommandStatus()
	},

	"help": func(ch *amqp.Channel, args Args) {
		gamelogic.PrintClientHelp()
	},

	"spam": func(ch *amqp.Channel, args Args) {
		fmt.Println("Spamming not allowed yet!")
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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		slog.Error("failed to welcome the client", "err", err)
		panic(err)
	}

	pubsub.DeclareAndBind(
		mqch,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.ExchangePerilDirect,
		routing.PauseKey,
		pubsub.QueueTypeTransient,
	)

	state := gamelogic.NewGameState(username)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		cmd := strings.ToLower(strings.TrimSpace(words[0]))
		if cmd == "exit" {
			gamelogic.PrintQuit()
			break
		}

		if fn, ok := commands[cmd]; ok {
			args := Args{
				State: state,
				Words: words,
			}
			fn(mqch, args)
			continue
		}

		fmt.Println("what ..")

	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch

	slog.Info("shutting down")
}
