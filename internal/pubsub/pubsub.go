package pubsub

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body: data,
	})

	if err != nil {
		slog.Error("failed to publish message", "err", err, "exchange", exchange, "key", key, "val", val)
	} else {
		slog.Debug("published message", "exchange", exchange, "key", key, "val", val)
	}

	return err
}
