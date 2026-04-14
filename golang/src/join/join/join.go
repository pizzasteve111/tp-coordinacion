package join

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type joinBatch struct {
	Tasks  int                   `json:"tasks"`
	Fruits []fruititem.FruitItem `json:"fruits"`
}
type joinStorage struct {
	mu      sync.Mutex
	dirPath string
}

func newJoinStorage() *joinStorage {
	dirPath := "/tmp/join"
	os.MkdirAll(dirPath, 0755)
	return &joinStorage{dirPath: dirPath}
}

func (s *joinStorage) filePath(clientId string) string {
	return fmt.Sprintf("%s/%s.json", s.dirPath, clientId)
}

func (s *joinStorage) AppendBatch(clientId string, batch joinBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.filePath(clientId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	line, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	_, err = f.Write(append(line, '\n'))
	return err
}

// FlushAndClear: streaming NDJSON, llama fn por cada batch, borra el archivo al final
func (s *joinStorage) FlushAndClear(clientId string, fn func(joinBatch) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.filePath(clientId))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	dec := json.NewDecoder(f)
	for dec.More() {
		var batch joinBatch
		if err := dec.Decode(&batch); err != nil {
			f.Close()
			return err
		}
		if err := fn(batch); err != nil {
			f.Close()
			return err
		}
	}
	f.Close()
	return os.Remove(s.filePath(clientId))
}

type Join struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	storage     *joinStorage
	topAmount   int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{inputQueue: inputQueue, outputQueue: outputQueue, storage: newJoinStorage(), topAmount: config.TopSize}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruits, clientId, totalTasks, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing", "err", err)
		return
	}

	if isEof {
		join.handleEof(clientId, totalTasks)
		return
	}

	// Guardar el batch parcial del cliente
	if err := join.storage.AppendBatch(clientId, joinBatch{
		Tasks:  totalTasks,
		Fruits: fruits,
	}); err != nil {
		slog.Error("While appending batch", "clientId", clientId, "err", err)
		return
	}
}

func (join *Join) handleEof(clientId string, totalTasks int) {

	// Generar top global a partir de todos los tops parciales
	aggregated := map[string]fruititem.FruitItem{}
	err := join.storage.FlushAndClear(clientId, func(batch joinBatch) error {
		for _, fruit := range batch.Fruits {
			if existing, ok := aggregated[fruit.Fruit]; ok {
				aggregated[fruit.Fruit] = existing.Sum(fruit)
			} else {
				aggregated[fruit.Fruit] = fruit
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("While flushing storage", "clientId", clientId, "err", err)
		return
	}

	// Ordenar y tomar top N
	fruits := make([]fruititem.FruitItem, 0, len(aggregated))
	for _, item := range aggregated {
		fruits = append(fruits, item)
	}
	sort.Slice(fruits, func(i, j int) bool {
		return fruits[j].Less(fruits[i]) // descendente por Amount
	})
	if len(fruits) > join.topAmount {
		fruits = fruits[:join.topAmount]
	}

	// Enviar resultado al gateway con clientId para que rutee al cliente correcto
	msg, err := inner.SerializeMessage(fruits, clientId, totalTasks)
	if err != nil {
		slog.Error("While serializing top", "err", err)
		return
	}
	if err := join.outputQueue.Send(*msg); err != nil {
		slog.Error("While sending top", "err", err)
		return
	}

	slog.Info("Top sent to gateway", "clientId", clientId, "top", fruits)
}
