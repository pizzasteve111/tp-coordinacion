package sum

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

//logica de storage de sum. Un directorio por sum con N storages por client y sum.

type sumStorage struct {
	mu      sync.Mutex
	dirPath string
}

func newSumStorage(id int) *sumStorage {
	dirPath := fmt.Sprintf("/tmp/sum_%d", id)
	os.MkdirAll(dirPath, 0755)
	return &sumStorage{dirPath: dirPath}
}

func (s *sumStorage) filePath(clientId string) string {
	return fmt.Sprintf("%s/%s.json", s.dirPath, clientId)
}

func (s *sumStorage) readSumClient(clientId string) ([]fruititem.FruitItem, error) {
	result := []fruititem.FruitItem{}
	data, err := os.ReadFile(s.filePath(clientId))
	if os.IsNotExist(err) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *sumStorage) writeSumClient(clientId string, items []fruititem.FruitItem) error {
	bytes, err := json.Marshal(items)
	if err != nil {
		return err
	}
	return os.WriteFile(s.filePath(clientId), bytes, 0644)
}

type Sum struct {
	//esto para cuando escalemos a varios sums
	id         string
	inputQueue middleware.Middleware
	//por este exchange los sums reciben EoF de client x
	outputExchange middleware.Middleware
	fruitItemMap   map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage() error {
	slog.Info("Received End Of Records message")
	for key := range sum.fruitItemMap {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[key]}
		message, err := inner.SerializeMessage(fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem) error {
	for _, fruitRecord := range fruitRecords {
		_, ok := sum.fruitItemMap[fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[fruitRecord.Fruit] = sum.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
