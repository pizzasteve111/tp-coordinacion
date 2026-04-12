package middleware

import (
	a "github.com/rabbitmq/amqp091-go"

	"fmt"
)

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}

	// declarar la cola (si no existe la crea)
	//hago que la cola sea persistente ante restarts
	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	//pongo prefetch en 1, aseguro que no haya mas mensajes de los que puedo procesar
	if err = ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	return &WorkQueueMiddleware{
		QueueName:  queueName,
		Connection: conn,
		Channel:    ch,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	if err = ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	for _, key := range keys {
		err = ch.QueueBind(
			q.Name,
			key,
			exchange,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	return &ExchangeMiddleware{
		Exchange:   exchange,
		Keys:       keys,
		Connection: conn,
		Channel:    ch,
		QueueName:  q.Name,
	}, nil
}

func connect(settings ConnSettings) (*a.Connection, *a.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)

	conn, err := a.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}
