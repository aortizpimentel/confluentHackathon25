package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/linkedin/goavro/v2"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---------------- Schema Registry client ----------------

type schemaRegistryClient struct {
	url    string
	key    string
	secret string
	client *http.Client
}

type srSchemaResponse struct {
	Schema  string `json:"schema"`
	Id      int    `json:"id"`
	Version int    `json:"version"`
	Subject string `json:"subject"`
}

func newSRClient() *schemaRegistryClient {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	key := os.Getenv("SR_API_KEY")
	secret := os.Getenv("SR_API_SECRET")

	if url == "" || key == "" || secret == "" {
		fmt.Println("[FATAL] SCHEMA_REGISTRY_URL / SR_API_KEY / SR_API_SECRET must be set")
		os.Exit(1)
	}

	return &schemaRegistryClient{
		url:    url,
		key:    key,
		secret: secret,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *schemaRegistryClient) getLatestSchema(subject string) (int, string, error) {
	u := fmt.Sprintf("%s/subjects/%s/versions/latest", c.url, subject)

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return 0, "", err
	}
	req.SetBasicAuth(c.key, c.secret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return 0, "", fmt.Errorf("schema registry error %d: %s", resp.StatusCode, string(body))
	}

	var r srSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, "", err
	}

	return r.Id, r.Schema, nil
}

// ---------------- Avro helpers ----------------

type avroEncoder struct {
	schemaID int
	codec    *goavro.Codec
}

func newAvroEncoder(sr *schemaRegistryClient, subject string) (*avroEncoder, error) {
	id, schema, err := sr.getLatestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("getLatestSchema(%s): %w", subject, err)
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("NewCodec(%s): %w", subject, err)
	}

	return &avroEncoder{
		schemaID: id,
		codec:    codec,
	}, nil
}

// encodeRecord genera el payload con formato Confluent:
// magic byte (0) + schemaID (int32 big endian) + Avro binario
func (e *avroEncoder) encodeRecord(record map[string]interface{}) ([]byte, error) {
	native := record

	bin, err := e.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("BinaryFromNative: %w", err)
	}

	header := make([]byte, 5)
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:], uint32(e.schemaID))

	return append(header, bin...), nil
}

// ---------------- Kafka config ----------------

func kafkaConfigMap() *kafka.ConfigMap {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
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
	} else {
		fmt.Println("[WARN] KAFKA_API_KEY / KAFKA_API_SECRET not set, using PLAINTEXT (local/dev only)")
	}

	return cfg
}

// ---------------- main ----------------

func main() {
	ctx := context.Background()

	admissionTopic := getenv("KAFKA_ADMISSION_TOPIC", "er.admission.requests")
	triageTopic := getenv("KAFKA_TRIAGE_TOPIC", "er.triage.completed")

	// Schema Registry client
	sr := newSRClient()

	// Avro encoders para los dos subjects
	admissionEncoder, err := newAvroEncoder(sr, "er.admission.requests-value")
	if err != nil {
		panic(fmt.Sprintf("error creating admission encoder: %v", err))
	}

	triageEncoder, err := newAvroEncoder(sr, "er.triage.completed-value")
	if err != nil {
		panic(fmt.Sprintf("error creating triage encoder: %v", err))
	}

	// Kafka producer
	producer, err := kafka.NewProducer(kafkaConfigMap())
	if err != nil {
		panic(fmt.Sprintf("failed to create producer: %v", err))
	}
	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Println("[ERROR] delivery failed:", ev.TopicPartition.Error)
				}
			}
		}
	}()

	rand.Seed(time.Now().UnixNano())

	fmt.Println("[INFO] simulator (Avro) started")
	fmt.Printf("[INFO] Topics: admission=%s, triage=%s\n", admissionTopic, triageTopic)

	for i := 0; i < 50; i++ {
		episodeId := fmt.Sprintf("E%05d", i+1)
		patientId := fmt.Sprintf("P%04d", rand.Intn(9999))

		// Admisión hace entre 0 y 40 min
		arrivalTs := time.Now().Add(-time.Duration(rand.Intn(40)) * time.Minute)
		arrivalMillis := arrivalTs.UnixMilli()

		// ----- Mensaje Avro para er.admission.requests -----
		admissionRecord := map[string]interface{}{
			"episodeId":   episodeId,
			"patientId":   patientId,
			"arrivalMode": "WALK_IN",
			// Schema Avro: long + logicalType timestamp-millis -> int64 epoch millis
			"arrivalTs": arrivalMillis,
		}

		admissionPayload, err := admissionEncoder.encodeRecord(admissionRecord)
		if err != nil {
			fmt.Println("[ERROR] encoding admission record:", err)
			continue
		}

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &admissionTopic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(episodeId),
			Value: admissionPayload,
		}, nil)
		if err != nil {
			fmt.Println("[ERROR] producing admission:", err)
		}

		// ----- Mensaje Avro para er.triage.completed -----
		triageLevels := []string{"I", "II", "III", "IV"}
		lvl := triageLevels[rand.Intn(len(triageLevels))]

		// Espera aleatoria; algunos romperán SLA
		waitMin := rand.Intn(90)
		triageTs := arrivalTs.Add(time.Duration(waitMin) * time.Minute)
		triageMillis := triageTs.UnixMilli()

		triageRecord := map[string]interface{}{
			"episodeId":   episodeId,
			"triageLevel": lvl,
			"reason":      "CHEST_PAIN",
			"triageTs":    triageMillis,
		}

		triagePayload, err := triageEncoder.encodeRecord(triageRecord)
		if err != nil {
			fmt.Println("[ERROR] encoding triage record:", err)
			continue
		}

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &triageTopic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(episodeId),
			Value: triagePayload,
		}, nil)
		if err != nil {
			fmt.Println("[ERROR] producing triage:", err)
		}
	}

	// Esperar a que se entreguen los mensajes
	producer.Flush(10 * 1000)

	fmt.Println("[INFO] sample ER Avro events produced")
	<-ctx.Done()
}
