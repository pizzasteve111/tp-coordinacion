package middleware

import (
	"fmt"
	"sync/atomic"

	ampq "github.com/rabbitmq/amqp091-go"
)

// pongo un contador global para el caso de que varios consumers sean los mismos
// así el tag no se repite
var queueConsumerCounter uint64

// lo unico que conoce es el nombre de la queue que le interesa consumir y el stream para hablar con el broker
type WorkQueueMiddleware struct {
	//este mailbox es donde nos guardamos los msjs que vamos recibiendo

	QueueName  string
	Connection *ampq.Connection
	Channel    *ampq.Channel
	tag        string
}

// desencola el fifo y se lo pasa a su callback. Si callback dev
func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	//soluciono que no se repitan los tags entre consumers
	id := atomic.AddUint64(&queueConsumerCounter, 1)
	tag := fmt.Sprintf("%s-consumer-%d", q.QueueName, id)
	q.tag = tag

	msgs, err := q.Channel.Consume(
		q.QueueName,
		tag, // tag fijo y conocido
		false, false, false, false, nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	//esto es bloqueante por ser de la go routine
	//vigilar el caso en el que hago stop consume antes de leer msgs
	for d := range msgs {
		msg := Message{Body: string(d.Body)}
		ack := func() { d.Ack(false) }
		nack := func() { d.Nack(false, true) }
		callbackFunc(msg, ack, nack)
	}
	return nil
}

func (q *WorkQueueMiddleware) StopConsuming() error {
	//Manda el mensaje SCQ y la queue name al broker, este lo va a descatalogar como consumidores
	//broker responde con el mensaje SCQ, que se va a leer desde el start consuming y se va a cortar la escucha
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if err := q.Channel.Cancel(q.tag, false); err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil

}

func (q *WorkQueueMiddleware) Send(msg Message) error {
	//sobre la conexión que ya tiene, manda el mensaje con el header PUB
	//devuelve error si no había conexion
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if err := q.Channel.Publish(
		"",
		q.QueueName,
		false,
		false,
		ampq.Publishing{
			Body: []byte(msg.Body),
		},
	); err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *WorkQueueMiddleware) Close() error {
	//Manda header CLS donde avisa que cierra conn y que entonces
	//broker deje de tenerlo en cuenta.
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if err := q.Channel.Close(); err != nil {
		return err
	}
	return q.Connection.Close()

}
