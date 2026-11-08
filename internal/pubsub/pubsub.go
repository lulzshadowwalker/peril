package pubsub
import (
	"context"
	"encoding/json"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})

	if err != nil {
		slog.Error("failed to publish message", "err", err, "exchange", exchange, "key", key, "val", val)
	} else {
		slog.Debug("published message", "exchange", exchange, "key", key, "val", val)
	}

	return err
}

type QueueType int

const (
	QueueTypeDurable QueueType = iota
	QueueTypeTransient
)

type QueueConfig struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]any
}

func (t QueueType) Config() QueueConfig {
	if t == QueueTypeTransient {
		return QueueConfig{AutoDelete: true, Exclusive: true}
	}

	return QueueConfig{Durable: true}
}

func DeclareAndBind(ch *amqp.Channel, name, exchange, key string, t QueueType) (amqp.Queue, error) {
	c := t.Config()
	q, err := ch.QueueDeclare(name, c.Durable, c.AutoDelete, c.Exclusive, c.NoWait, c.Args)
	if err != nil {
		return amqp.Queue{}, err
	}

	err = ch.QueueBind(name, key, exchange, c.NoWait, nil)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}
