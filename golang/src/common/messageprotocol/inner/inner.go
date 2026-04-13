package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type envelope struct {
	ClientId   string
	TotalTasks int
	Records    [][]interface{}
}

// me queda inutil porque trabajo todo sobre envelopes
func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(fruitRecords []fruititem.FruitItem, client_id string, total_tasks int) (*middleware.Message, error) {

	records := [][]interface{}{}
	for _, r := range fruitRecords {
		records = append(records, []interface{}{r.Fruit, r.Amount})
	}
	//convierto el struct en un Json
	body, err := json.Marshal(envelope{
		ClientId:   client_id,
		TotalTasks: total_tasks,
		Records:    records,
	})
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

// recibo el top, pero ademas debo conocer
func DeserializeMessage(message *middleware.Message) ([]fruititem.FruitItem, string, int, bool, error) {
	var env envelope
	if err := json.Unmarshal([]byte(message.Body), &env); err != nil {
		return nil, "", 0, false, err
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, pair := range env.Records {
		//recorro mi arreglo de pares de frutas
		fruit, ok := pair[0].(string)
		if !ok {
			return nil, "", 0, false, errors.New("fruit is not a string")
		}
		//pongo en 64 por si son valores exageradamente grandes
		fruitCount, ok := pair[1].(float64)
		if !ok {
			return nil, "", 0, false, errors.New("amount is not a number")
		}

		fruitRecords = append(fruitRecords, fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitCount)})
	}
	//si era lista vacía
	isEof := len(fruitRecords) == 0
	return fruitRecords, env.ClientId, env.TotalTasks, isEof, nil
}
