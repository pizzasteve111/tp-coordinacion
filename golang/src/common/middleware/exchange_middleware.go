package middleware

import (
	"fmt"
	"sync/atomic"

	a "github.com/rabbitmq/amqp091-go"
)

var exchangeConsumerCounter uint64

// lo unico que conoce es el nombre de la queue que le interesa consumir y el stream para hablar con el broker
type ExchangeMiddleware struct {
	//tiene un exchange al que se suscriben las queues
	Exchange string
	//keys sería el arreglo de nombres de colas asociadas
	Keys []string
	//el consumer crea su propia queue donde recibe los mensajes de sus bindings
	QueueName  string
	Connection *a.Connection
	Channel    *a.Channel
	//es para identificar al consumer
	//util para los close donde mejor especificamos quien cierra conn
	consumerTag string
}

// empiezo a escuchar lo que enrutea el exchange hacia mi queue
// el mensaje lo paso a la callback
func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	//error => si estaba desconectado el channel, que devuelva  ErrMessageMiddlewareDisconnected
	//un if channel esta desconected, devolvemos el error
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	id := atomic.AddUint64(&exchangeConsumerCounter, 1)
	e.consumerTag = fmt.Sprintf("%s-consumer-%d", e.Exchange, id)
	//ahora se puede identificar

	msgs, err := e.Channel.Consume(
		e.QueueName,
		e.consumerTag,
		false, false, false, false, nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	// bloqueante — el caller usa `go middleware.StartConsuming(...)`
	for d := range msgs {
		delivery := d
		msg := Message{Body: string(delivery.Body)}
		ack := func() { delivery.Ack(false) }
		nack := func() { delivery.Nack(false, true) }
		callbackFunc(msg, ack, nack)
	}
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	//if de si el channel ya esta desconectado ErrMessageMiddlewareDisconnected
	if err := e.Channel.Cancel(e.consumerTag, false); err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

// no mando mensaje a una queue, sino que pusheo a un exchange que luego lo
// distribuye a las keys asociadas
func (e *ExchangeMiddleware) Send(message Message) error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	keys := e.Keys
	if message.RoutingKey != "" {
		keys = []string{message.RoutingKey} // routing específico
	}
	for _, key := range keys {
		//si falla la key N, no se sabe cuantas de las 1..N-1 se enviaron bien
		err := e.Channel.Publish(
			e.Exchange,
			key,
			false,
			false,
			a.Publishing{
				Body: []byte(message.Body),
			},
		)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareClose
	}
	if err := e.Channel.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	if err := e.Connection.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
