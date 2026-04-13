package messagehandler

import (
	"crypto/rand"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	//añadimos total_tasks de ese client
	clientId   string
	totalTasks int
}

func NewMessageHandler() MessageHandler {
	//el client id es algo aleatorio, solo se hace para identificarse
	//8! muchas combinaciones
	b := make([]byte, 8)
	rand.Read(b)
	return MessageHandler{clientId: fmt.Sprintf("%x", b)}
}

func (h *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	//aumentamos el total tasks por cada mensaje que serializamos para enviar
	h.totalTasks++
	data := []fruititem.FruitItem{fruitRecord}
	//no mando total tasks
	return inner.SerializeMessage(data, h.clientId, 0)
}

func (h *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	//no mandamos el data que es solo uina lista vacía, mandamos el total tasks
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(data, h.clientId, h.totalTasks)
}

func (h *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, id, _, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	//me llego el top de otro client
	if id != h.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}
