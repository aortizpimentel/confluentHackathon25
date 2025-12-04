package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/linkedin/goavro/v2"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

// =====================================================
// Helpers
// =====================================================

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// =====================================================
// Schema Registry client (decode Confluent Avro)
// =====================================================

type schemaRegistryClient struct {
	url, key, secret string

	httpClient *http.Client
	mu         sync.RWMutex
	codecs     map[int]*goavro.Codec
}

type srSchemaByIDResponse struct {
	Schema string `json:"schema"`
}

func newSRClient() *schemaRegistryClient {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	key := os.Getenv("SR_API_KEY")
	secret := os.Getenv("SR_API_SECRET")

	if url == "" || key == "" || secret == "" {
		log.Fatalf("SCHEMA_REGISTRY_URL, SR_API_KEY and SR_API_SECRET must be set")
	}

	return &schemaRegistryClient{
		url:        url,
		key:        key,
		secret:     secret,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		codecs:     make(map[int]*goavro.Codec),
	}
}

func (c *schemaRegistryClient) getCodec(schemaID int) (*goavro.Codec, error) {
	c.mu.RLock()
	if codec, ok := c.codecs[schemaID]; ok {
		c.mu.RUnlock()
		return codec, nil
	}
	c.mu.RUnlock()

	u := fmt.Sprintf("%s/schemas/ids/%d", c.url, schemaID)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.key, c.secret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error %d: %s", resp.StatusCode, string(body))
	}

	var r srSchemaByIDResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}

	codec, err := goavro.NewCodec(r.Schema)
	if err != nil {
		return nil, fmt.Errorf("NewCodec: %w", err)
	}

	c.mu.Lock()
	c.codecs[schemaID] = codec
	c.mu.Unlock()

	return codec, nil
}

// Expect Confluent Avro wire format: magic byte (0) + schemaID (int32) + Avro binary
func (c *schemaRegistryClient) decodeConfluentAvro(payload []byte) (map[string]interface{}, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("payload too short for Confluent Avro")
	}

	if payload[0] != 0 {
		return nil, fmt.Errorf("unexpected magic byte %d", payload[0])
	}

	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	codec, err := c.getCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("getCodec(%d): %w", schemaID, err)
	}

	native, _, err := codec.NativeFromBinary(payload[5:])
	if err != nil {
		return nil, fmt.Errorf("NativeFromBinary: %w", err)
	}

	record, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected Avro record, got %T", native)
	}
	return record, nil
}

// =====================================================
// Avro union helpers
// =====================================================

func avroString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]interface{}:
		for _, inner := range t {
			if s, ok := inner.(string); ok {
				return s
			}
		}
	}
	return ""
}

func avroInt(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	case map[string]interface{}:
		for _, inner := range t {
			return avroInt(inner)
		}
	}
	return 0
}

func avroFloat64(v interface{}) float64 {
	switch t := v.(type) {
	case float32:
		return float64(t)
	case float64:
		return t
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case map[string]interface{}:
		for _, inner := range t {
			return avroFloat64(inner)
		}
	}
	return 0
}

func avroTimeFromMillis(v interface{}) time.Time {
	// value is usually logicalType timestamp-millis
	ms := int64(avroInt(v))
	if ms <= 0 {
		return time.Time{}
	}
	sec := ms / 1000
	nsec := (ms % 1000) * int64(time.Millisecond)
	return time.Unix(sec, nsec).UTC()
}

// =====================================================
// In-memory load stats store
// =====================================================

type loadStatsStore struct {
	mu   sync.RWMutex
	data map[string]int // triageLevel -> waitingForDoctor
}

func newLoadStatsStore() *loadStatsStore {
	return &loadStatsStore{
		data: make(map[string]int),
	}
}

func (s *loadStatsStore) set(level string, waiting int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[level] = waiting
}

func (s *loadStatsStore) get(level string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[level]
}

// =====================================================
// Kafka config
// =====================================================

func kafkaConsumerConfig(groupID string) *kafka.ConfigMap {
	brokers := getenv("KAFKA_BOOTSTRAP", "localhost:9092")
	apiKey := os.Getenv("KAFKA_API_KEY")
	apiSecret := os.Getenv("KAFKA_API_SECRET")

	cfg := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}

	if apiKey != "" && apiSecret != "" {
		_ = cfg.SetKey("security.protocol", "SASL_SSL")
		_ = cfg.SetKey("sasl.mechanisms", "PLAIN")
		_ = cfg.SetKey("sasl.username", apiKey)
		_ = cfg.SetKey("sasl.password", apiSecret)
	} else {
		log.Println("[WARN] KAFKA_API_KEY / KAFKA_API_SECRET not set, using PLAINTEXT (dev only!)")
	}

	return cfg
}

func kafkaProducerConfig() *kafka.ConfigMap {
	brokers := getenv("KAFKA_BOOTSTRAP", "localhost:9092")
	apiKey := os.Getenv("KAFKA_API_KEY")
	apiSecret := os.Getenv("KAFKA_API_SECRET")

	cfg := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
	}

	if apiKey != "" && apiSecret != "" {
		_ = cfg.SetKey("security.protocol", "SASL_SSL")
		_ = cfg.SetKey("sasl.mechanisms", "PLAIN")
		_ = cfg.SetKey("sasl.username", apiKey)
		_ = cfg.SetKey("sasl.password", apiSecret)
	}

	return cfg
}

// =====================================================
// Insight event and OpenAI prompt
// =====================================================

type InsightEvent struct {
	EpisodeID              string    `json:"episodeId"`
	PatientID              string    `json:"patientId"`
	TriageLevel            string    `json:"triageLevel"`
	WaitMinutes            float64   `json:"waitMinutes"`
	SLAMinutes             int       `json:"slaMinutes"`
	Severity               string    `json:"severity"`
	Gender                 string    `json:"gender,omitempty"`
	RiskGroup              string    `json:"riskGroup,omitempty"`
	CurrentWaitingForLevel int       `json:"currentWaitingForLevel"`
	ModelSummary           string    `json:"modelSummary"`
	ViolationTime          time.Time `json:"violationTime"`
	GeneratedAt            time.Time `json:"generatedAt"`
}

// Build the prompt fed to OpenAI
func buildInsightPrompt(e InsightEvent) string {
	vt := e.ViolationTime.Format(time.RFC3339)

	return fmt.Sprintf(
		"You are an AI assistant helping ER coordinators manage emergency room load and SLA breaches.\n\n"+
			"Given the following real-time data, write a short summary (2-3 sentences) and one actionable recommendation.\n"+
			"Focus on patient safety and operational efficiency. Answer in concise English.\n\n"+
			"Episode ID: %s\n"+
			"Patient ID: %s\n"+
			"Triage level: %s\n"+
			"Wait time (minutes): %.1f\n"+
			"SLA (minutes): %d\n"+
			"Severity flag: %s\n"+
			"Current queue size for this triage level: %d\n"+
			"Patient gender: %s\n"+
			"Patient risk group: %s\n"+
			"Violation timestamp (UTC): %s\n",
		e.EpisodeID,
		e.PatientID,
		e.TriageLevel,
		e.WaitMinutes,
		e.SLAMinutes,
		e.Severity,
		e.CurrentWaitingForLevel,
		e.Gender,
		e.RiskGroup,
		vt,
	)
}

// =====================================================
// Consumers
// =====================================================

func runLoadStatsConsumer(ctx context.Context, sr *schemaRegistryClient, store *loadStatsStore) error {
	topic := getenv("KAFKA_LOAD_TOPIC", "er.load.stats")

	c, err := kafka.NewConsumer(kafkaConsumerConfig("er-insights-load-stats"))
	if err != nil {
		return fmt.Errorf("load stats consumer: %w", err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		return fmt.Errorf("SubscribeTopics(load stats): %w", err)
	}

	log.Printf("[load-stats] Subscribed to topic %s", topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("[load-stats] Context cancelled, stopping consumer")
			return nil
		default:
		}

		ev := c.Poll(500)
		if ev == nil {
			continue
		}

		switch m := ev.(type) {
		case *kafka.Message:
			if m.Value == nil {
				continue
			}

			// Key is Avro with triageLevel; value contains waitingForDoctor
			var triageLevel string

			if len(m.Key) > 0 {
				recKey, err := sr.decodeConfluentAvro(m.Key)
				if err != nil {
					log.Printf("[load-stats] decode key error: %v", err)
				} else {
					triageLevel = avroString(recKey["triageLevel"])
				}
			}

			recVal, err := sr.decodeConfluentAvro(m.Value)
			if err != nil {
				log.Printf("[load-stats] decode value error: %v", err)
				continue
			}

			// Fallback: some schemas might include triageLevel also in value
			if triageLevel == "" {
				triageLevel = avroString(recVal["triageLevel"])
			}
			if triageLevel == "" {
				log.Printf("[load-stats] missing triageLevel in key/value")
				continue
			}

			waitingForDoctor := avroInt(recVal["waitingForDoctor"])

			store.set(triageLevel, waitingForDoctor)

			log.Printf("[load-stats] triage=%s waitingForDoctor=%d", triageLevel, waitingForDoctor)

		case kafka.Error:
			log.Printf("[load-stats] Kafka error: %v", m)
		default:
			// ignore
		}
	}
}

func runViolationsConsumer(
	ctx context.Context,
	sr *schemaRegistryClient,
	store *loadStatsStore,
	openaiClient openai.Client,
	producer *kafka.Producer,
) error {
	violationsTopic := getenv("KAFKA_VIOLATIONS_TOPIC", "er.violations.enriched")
	insightsTopic := getenv("KAFKA_INSIGHTS_TOPIC", "er.insights.ai")

	c, err := kafka.NewConsumer(kafkaConsumerConfig("er-insights-violations"))
	if err != nil {
		return fmt.Errorf("violations consumer: %w", err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{violationsTopic}, nil); err != nil {
		return fmt.Errorf("SubscribeTopics(violations): %w", err)
	}

	log.Printf("[violations] Subscribed to topic %s", violationsTopic)
	log.Printf("[violations] Insights will be produced to topic %s", insightsTopic)

	for {
		select {
		case <-ctx.Done():
			log.Println("[violations] Context cancelled, stopping consumer")
			return nil
		default:
		}

		ev := c.Poll(500)
		if ev == nil {
			continue
		}

		switch m := ev.(type) {
		case *kafka.Message:
			if m.Value == nil {
				continue
			}

			record, err := sr.decodeConfluentAvro(m.Value)
			if err != nil {
				log.Printf("[violations] decode error: %v", err)
				continue
			}

			episodeID := avroString(record["episodeId"])
			triageLevel := avroString(record["triageLevel"])
			patientID := avroString(record["patientId"])
			waitMinutes := avroFloat64(record["waitMinutes"])
			slaMinutes := avroInt(record["slaMinutes"])
			severity := avroString(record["severity"])
			gender := avroString(record["gender"])
			riskGroup := avroString(record["riskGroup"])
			violationTs := avroTimeFromMillis(record["violationTs"])

			if triageLevel == "" {
				triageLevel = "UNKNOWN"
			}

			currentQueue := store.get(triageLevel)

			// Build insight event
			insight := InsightEvent{
				EpisodeID:              episodeID,
				PatientID:              patientID,
				TriageLevel:            triageLevel,
				WaitMinutes:            math.Round(waitMinutes*10) / 10,
				SLAMinutes:             slaMinutes,
				Severity:               severity,
				Gender:                 gender,
				RiskGroup:              riskGroup,
				CurrentWaitingForLevel: currentQueue,
				ViolationTime:          violationTs,
				GeneratedAt:            time.Now().UTC(),
			}

			// Call OpenAI to generate summary
			summary, err := callOpenAI(ctx, openaiClient, insight)
			if err != nil {
				log.Printf("[violations] OpenAI error: %v", err)
				insight.ModelSummary = fmt.Sprintf("AI generation failed: %v", err)
			} else {
				insight.ModelSummary = summary
			}

			// Produce JSON to er.insights.ai
			payload, err := json.Marshal(insight)
			if err != nil {
				log.Printf("[violations] marshal insight error: %v", err)
				continue
			}

			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &insightsTopic,
					Partition: kafka.PartitionAny,
				},
				Value: payload,
			}, nil)
			if err != nil {
				log.Printf("[violations] produce error: %v", err)
				continue
			}

			log.Printf("[violations] Produced insight for episode=%s triage=%s queue=%d",
				episodeID, triageLevel, currentQueue)

		case kafka.Error:
			log.Printf("[violations] Kafka error: %v", m)
		default:
			// ignore other event types
		}
	}
}

// =====================================================
// OpenAI call
// =====================================================

func newOpenAIClient() openai.Client {
	var opts []option.RequestOption

	// API key por defecto la coge de OPENAI_API_KEY si no pasas nada
	if base := os.Getenv("OPENAI_BASE_URL"); base != "" {
		opts = append(opts, option.WithBaseURL(base))
	}

	return openai.NewClient(opts...)
}

func callOpenAI(ctx context.Context, client openai.Client, e InsightEvent) (string, error) {
	model := getenv("OPENAI_MODEL", "gpt-5-mini")

	prompt := buildInsightPrompt(e)

	resp, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage("You are a concise and clinically-aware ER operations assistant."),
			openai.UserMessage(prompt),
		},
	})
	if err != nil {
		return "", err
	}

	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("no choices returned from OpenAI")
	}

	return resp.Choices[0].Message.Content, nil
}

// =====================================================
// main
// =====================================================

func main() {
	log.Println("[startup] er-insights-service starting up")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srClient := newSRClient()
	loadStore := newLoadStatsStore()
	openaiClient := newOpenAIClient()

	producer, err := kafka.NewProducer(kafkaProducerConfig())
	if err != nil {
		log.Fatalf("failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Drain producer delivery reports in the background
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("[producer] delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()

	// Start consumers
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runLoadStatsConsumer(ctx, srClient, loadStore); err != nil {
			log.Printf("load stats consumer exited with error: %v", err)
			stop()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runViolationsConsumer(ctx, srClient, loadStore, openaiClient, producer); err != nil {
			log.Printf("violations consumer exited with error: %v", err)
			stop()
		}
	}()

	log.Println("[startup] er-insights-service is running. Waiting for signals...")
	wg.Wait()
	log.Println("[shutdown] er-insights-service stopped")
}
