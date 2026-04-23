package sum

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os/signal"
	"syscall"

	"log/slog"
	"os"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type syncMsg struct {
	Type       string `json:"type"` // "eof" | "ready"
	ClientId   string `json:"client_id"`
	TotalTasks int    `json:"total_tasks,omitempty"` // solo en "eof"
	SumId      int    `json:"sum_id,omitempty"`      // solo en "ready"
	MyTasks    int    `json:"my_tasks,omitempty"`    // solo en "ready"
}
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

// devuelve el tamaño del archivo así sabemos si hay que mandar a flushear
func (s *sumStorage) Append(clientId string, items []fruititem.FruitItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.filePath(clientId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, item := range items {
		line, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if _, err = f.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil

}

func (s *sumStorage) Delete(clientId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.Remove(s.filePath(clientId))
}

// función para limpiar archivos y no cargar en memoria todo
func (s *sumStorage) FlushAndClear(clientId string, fn func(fruititem.FruitItem) error) error {
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
		var item fruititem.FruitItem
		if err := dec.Decode(&item); err != nil {
			f.Close()
			return err
		}
		if err := fn(item); err != nil {
			f.Close()
			return err
		}
	}
	f.Close()
	return os.Remove(s.filePath(clientId))
}

type Sum struct {
	inputQueue middleware.Middleware

	outputExchange    middleware.Middleware
	fruitItemMap      map[string]fruititem.FruitItem
	storage           *sumStorage
	mu                sync.Mutex
	aggAmount         int
	aggregationPrefix string
	id                int
	sumAmount         int
	syncPublisher     middleware.Middleware
	syncConsumer      middleware.Middleware
	localTasks        map[string]int
	pendingEof        map[string]int
	readyCounts       map[string]map[int]int
	syncMu            sync.Mutex
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
	allSumKeys := make([]string, config.SumAmount)
	for i := range config.SumAmount {
		allSumKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	}
	syncExchangeName := config.SumPrefix + "_sync"
	syncPublisher, err := middleware.CreateExchangeMiddleware(syncExchangeName, allSumKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}
	ownKey := fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)
	syncConsumer, err := middleware.CreateExchangeMiddleware(syncExchangeName, []string{ownKey}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		syncPublisher.Close()
		return nil, err
	}
	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		fruitItemMap:      map[string]fruititem.FruitItem{},
		storage:           newSumStorage(config.Id),
		id:                config.Id,
		sumAmount:         config.SumAmount,
		aggAmount:         config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		syncPublisher:     syncPublisher,
		syncConsumer:      syncConsumer,
		localTasks:        map[string]int{}, pendingEof: map[string]int{}, readyCounts: map[string]map[int]int{},
	}, nil
}

func (sum *Sum) Run() {
	//habría que poner a consumir la output queue tambien
	go sum.handleSignals()
	go sum.consumeSync()
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) consumeSync() {
	sum.syncConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		defer ack()
		sum.handleSyncMessage(msg)
	})
}
func (sum *Sum) handleSyncMessage(msg middleware.Message) {
	var sm syncMsg
	if err := json.Unmarshal([]byte(msg.Body), &sm); err != nil {
		return
	}
	switch sm.Type {
	case "eof":
		sum.mu.Lock()
		sum.pendingEof[sm.ClientId] = sm.TotalTasks
		sum.mu.Unlock()
		sum.broadcastReady(sm.ClientId)
	case "ready":
		sum.mu.Lock()
		if sum.readyCounts[sm.ClientId] == nil {
			sum.readyCounts[sm.ClientId] = map[int]int{}
		}
		sum.readyCounts[sm.ClientId][sm.SumId] = sm.MyTasks
		total := 0
		for _, t := range sum.readyCounts[sm.ClientId] {
			total += t
		}
		expected, hasPending := sum.pendingEof[sm.ClientId]
		shouldProcess := hasPending && total >= expected
		if shouldProcess {
			delete(sum.pendingEof, sm.ClientId)
			delete(sum.readyCounts, sm.ClientId)
			delete(sum.localTasks, sm.ClientId)
		}
		sum.mu.Unlock()
		if shouldProcess {
			sum.flushClient(sm.ClientId)
			sum.sendEof(sm.ClientId, expected)
		}
	}
}
func (sum *Sum) broadcastSyncEof(clientId string, totalTasks int) {
	body, _ := json.Marshal(syncMsg{Type: "eof", ClientId: clientId, TotalTasks: totalTasks})
	sum.syncPublisher.Send(middleware.Message{Body: string(body)})
}
func (sum *Sum) broadcastReady(clientId string) {
	sum.mu.Lock()
	myTasks := sum.localTasks[clientId]
	sum.mu.Unlock()
	body, _ := json.Marshal(syncMsg{Type: "ready", ClientId: clientId, SumId: sum.id, MyTasks: myTasks})
	sum.syncPublisher.Send(middleware.Message{Body: string(body)})
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM received, stopping")
	sum.inputQueue.StopConsuming()
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, clientId, totalTasks, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		sum.broadcastSyncEof(clientId, totalTasks)
		return

	}
	sum.mu.Lock()
	if err := sum.storage.Append(clientId, fruitRecords); err != nil {
		sum.mu.Unlock()
		slog.Error("While appending to storage", "err", err)
		return
	}
	sum.localTasks[clientId] += len(fruitRecords)
	sum.mu.Unlock()

}

func (sum *Sum) flushClient(clientId string) {
	aggregated := map[string]fruititem.FruitItem{}
	fruitCounts := map[string]int{}

	err := sum.storage.FlushAndClear(clientId, func(item fruititem.FruitItem) error {
		fruitCounts[item.Fruit]++
		if existing, ok := aggregated[item.Fruit]; ok {
			aggregated[item.Fruit] = existing.Sum(item)
		} else {
			aggregated[item.Fruit] = item
		}
		return nil
	})
	if err != nil {
		slog.Error("While flushing client", "clientId", clientId, "err", err)
		return
	}
	if len(aggregated) == 0 {
		return // archivo vacío, nada que enviar
	}

	for _, item := range aggregated {
		//error: mandaba el total tasks por cada fruit item, como si cada uno valiese por ese total.
		//mando todas las frutas procesadas en un mensaje

		msg, err := inner.SerializeMessage([]fruititem.FruitItem{item}, clientId, fruitCounts[item.Fruit])
		if err != nil {
			slog.Error("While serializing flush message", "err", err)
			return
		}
		msg.RoutingKey = sum.aggregationKeyFor(item.Fruit)
		if err := sum.outputExchange.Send(*msg); err != nil {
			slog.Error("While sending flush message", "err", err)
			return
		}
	}
}

func (sum *Sum) sendEof(clientId string, totalTasks int) {
	msg, err := inner.SerializeMessage([]fruititem.FruitItem{}, clientId, totalTasks)
	if err != nil {
		slog.Error("While serializing EOF", "err", err)
		return
	}
	if err := sum.outputExchange.Send(*msg); err != nil {
		slog.Error("While sending EOF", "err", err)
	}
}

func (sum *Sum) aggregationKeyFor(fruit string) string {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	idx := h.Sum32() % uint32(sum.aggAmount)
	return fmt.Sprintf("%s_%d", sum.aggregationPrefix, idx)
}
