package aggregation

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	aggFlushTimeout = 10 * time.Second
)

// el resultado que recibo de sum, las ocurrencias de X frutas y las N tasks originales que corresponde a ese resultado.
type aggBatch struct {
	Tasks  int                   `json:"tasks"`
	Fruits []fruititem.FruitItem `json:"fruits"`
}

type aggStorage struct {
	mu      sync.Mutex
	dirPath string
}

func newAggStorage(id int) *aggStorage {
	dirPath := fmt.Sprintf("/tmp/agg_%d", id)
	os.MkdirAll(dirPath, 0755)
	return &aggStorage{dirPath: dirPath}
}

func (s *aggStorage) filePath(clientId string) string {
	return fmt.Sprintf("%s/%s.json", s.dirPath, clientId)
}

func (s *aggStorage) AppendBatch(clientId string, batch aggBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := []aggBatch{}
	data, err := os.ReadFile(s.filePath(clientId))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(data) > 0 {
		json.Unmarshal(data, &existing)
	}
	existing = append(existing, batch)
	bytes, err := json.Marshal(existing)
	if err != nil {
		return err
	}
	return os.WriteFile(s.filePath(clientId), bytes, 0644)

}

func (s *aggStorage) FlushAndClear(clientId string, fn func(aggBatch) error) error {
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
	dec.Token() // consume '['
	for dec.More() {
		var batch aggBatch
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

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue     middleware.Middleware
	inputExchange   middleware.Middleware
	topSize         int
	storage         *aggStorage
	timers          map[string]*time.Timer
	timersMu        sync.Mutex
	pendingEofTasks map[string]int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:     outputQueue,
		inputExchange:   inputExchange,
		topSize:         config.TopSize,
		storage:         newAggStorage(config.Id),
		timers:          map[string]*time.Timer{},
		pendingEofTasks: map[string]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignals()
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}
func (agg *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM received, stopping")
	agg.inputExchange.StopConsuming()
}
func (agg *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruits, clientId, totalTasks, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		agg.timersMu.Lock()
		agg.pendingEofTasks[clientId] = totalTasks
		agg.timersMu.Unlock()
		agg.resetTimer(clientId)
		agg.sendEof(clientId, totalTasks) // reenvía EOF a Join con totalTasks del gateway
		return
	}

	if err := agg.storage.AppendBatch(clientId, aggBatch{
		Tasks:  totalTasks,
		Fruits: fruits,
	}); err != nil {
		slog.Error("While appending batch", "err", err)
		return
	}

	agg.resetTimer(clientId)

}

func (agg *Aggregation) flushClient(clientId string) {
	aggregated := map[string]fruititem.FruitItem{}
	accumulatedTasks := 0

	err := agg.storage.FlushAndClear(clientId, func(batch aggBatch) error {
		accumulatedTasks += batch.Tasks
		for _, fruit := range batch.Fruits {
			if existing, ok := aggregated[fruit.Fruit]; ok {
				aggregated[fruit.Fruit] = existing.Sum(fruit)
			} else {
				aggregated[fruit.Fruit] = fruit
			}
		}
		return nil
	})
	if err != nil || accumulatedTasks == 0 {
		return
	}
	//no debe descartar todas las ocurrencias de frutas que no llegan al top
	top := agg.buildFruitTop(aggregated)

	msg, err := inner.SerializeMessage(top, clientId, accumulatedTasks)
	if err != nil {
		slog.Error("While serializing flush", "err", err)
		return
	}
	if err := agg.outputQueue.Send(*msg); err != nil {
		slog.Error("While sending flush", "err", err)
	}
}

func (agg *Aggregation) sendEof(clientId string, totalTasks int) {
	msg, err := inner.SerializeMessage([]fruititem.FruitItem{}, clientId, totalTasks)
	if err != nil {
		slog.Error("While serializing EOF", "err", err)
		return
	}
	agg.outputQueue.Send(*msg)
}

func (agg *Aggregation) buildFruitTop(aggregated map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregated))
	for _, item := range aggregated {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(agg.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (agg *Aggregation) resetTimer(clientId string) {
	agg.timersMu.Lock()
	defer agg.timersMu.Unlock()
	if t, ok := agg.timers[clientId]; ok {
		t.Stop()
	}
	agg.timers[clientId] = time.AfterFunc(aggFlushTimeout, func() {
		//logica luego de los 10s del ultimo mensaje de un client
		agg.timersMu.Lock()
		_, hasPending := agg.pendingEofTasks[clientId]
		if hasPending {
			delete(agg.pendingEofTasks, clientId)
		}
		delete(agg.timers, clientId)
		agg.timersMu.Unlock()

		if !hasPending {
			// Llegaron datos pero todavía no se recibió EOF: no flushear aún
			return
		}
		agg.flushClient(clientId)
	})
}

func (agg *Aggregation) cancelTimer(clientId string) {
	agg.timersMu.Lock()
	defer agg.timersMu.Unlock()
	if t, ok := agg.timers[clientId]; ok {
		t.Stop()
		delete(agg.timers, clientId)
	}
}
