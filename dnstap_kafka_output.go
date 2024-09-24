/*
 * Copyright (c) 2019 Manabu Sonoda
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dtap

import (
	_ "embed"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/goccy/go-json"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/golang/protobuf/proto"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"

	"github.com/dangkaka/go-kafka-avro"
	"github.com/linkedin/goavro"

	"github.com/Shopify/sarama"
)

//go:embed assets/flat.avsc
var schemaStr string

type KafkaClient interface {
	Add(string, string, []byte, []byte) error
}

type DnstapKafkaOutput struct {
	config           *OutputKafkaConfig
	kafkaConfig      *sarama.Config
	producer         sarama.AsyncProducer
	registry         *kafka.CachedSchemaRegistryClient
	valueCodec       *goavro.Codec
	valueSchemaID    []byte
	keyCodec         *goavro.Codec
	keySchemaID      []byte
	errorLoggerClose chan bool
}

func NewDnstapKafkaOutput(config *OutputKafkaConfig, params *DnstapOutputParams) (Output, error) {
	// TODO: check
	sarama.Logger = log.New()
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = "PERT-DNSTAP"

	kafkaConfig.Producer.Return.Successes = false //no confirme los ok
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Retry.Max = 1 // int(config.GetRetry())
	kafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner

	// PERT-NB prueba sasl plain
	// kafkaConfig.Net.SASL.User = "user_dtap"
	// kafkaConfig.Net.SASL.Password = "#dn5t4p"
	// kafkaConfig.Net.SASL.Handshake = true
	// kafkaConfig.Net.SASL.Enable = true
	log.Info("Config Kafka dtap-TECO")
	//PERT-NB prueba metricas
	if config.Metrics {
		kafkaConfig.MetricRegistry = metrics.NewPrefixedChildRegistry(metrics.DefaultRegistry, "sarama.")
	}
	//PERT-NB prueba compress
	// NO ANDA kafkaConfig.Producer.Compression = sarama.CompressionZSTD
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy

	keyCodec, err := goavro.NewCodec(`{"type": "string"}`)
	if err != nil {
		return nil, err
	}

	valueCodec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, err
	}

	params.Handler = &DnstapKafkaOutput{
		config:      config,
		kafkaConfig: kafkaConfig,
		keyCodec:    keyCodec,
		valueCodec:  valueCodec,
	}

	return NewDnstapOutput(params), nil
}

func (o *DnstapKafkaOutput) open() error {
	var err error
	o.producer, err = sarama.NewAsyncProducer(o.config.Hosts, o.kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	o.errorLoggerClose = make(chan bool, 1)

	go func() {
		for err := range o.producer.Errors() {
			log.Info("Failed to write to kafka:", err)
		}
		o.errorLoggerClose <- true
	}()

	if o.config.GetOutputType() == "avro" {
		if o.valueSchemaID, err = o.getSchemaID(o.config.GetTopic()+"-value", o.valueCodec); err != nil {
			return fmt.Errorf("failed to get schema id: %w", err)
		}
		if o.keySchemaID, err = o.getSchemaID(o.config.GetTopic()+"-key", o.keyCodec); err != nil {
			return fmt.Errorf("failed to get schema id: %w", err)
		}
	}
	return nil
}
func (o *DnstapKafkaOutput) getSchemaID(subject string, codec *goavro.Codec) ([]byte, error) {
	registry := kafka.NewCachedSchemaRegistryClient(o.config.GetSchemaRegistries())
	schemaID, err := registry.CreateSubject(subject, codec)
	if err != nil {
		return nil, err
	}
	val := make([]byte, 4)
	binary.BigEndian.PutUint32(val, uint32(schemaID))
	return val, nil
}

func (o *DnstapKafkaOutput) GetEncoder(v interface{}, codec *goavro.Codec, schemaID []byte) (sarama.Encoder, error) {
	binary, err := codec.BinaryFromNative(nil, v)
	if err != nil {
		return nil, err
	}
	var binaryMsg []byte
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	//4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, schemaID...)
	//avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binary...)

	return sarama.ByteEncoder(binaryMsg), nil
}

func (o *DnstapKafkaOutput) write(frame []byte) error {
	var v, k sarama.Encoder
	if o.config.GetOutputType() == "protobuf" {
		k = sarama.ByteEncoder(o.config.GetKey())
		v = sarama.ByteEncoder(frame)
	} else {
		dt := dnstap.Dnstap{}
		if err := proto.Unmarshal(frame, &dt); err != nil {
			return err
		}
		data, err := FlatDnstap(&dt, &o.config.Flat)
		if err != nil {
			return err
		}
		if o.config.GetOutputType() == "avro" {
			var err error
			mapString := data.ToMapString()
			if v, err = o.GetEncoder(mapString, o.valueCodec, o.valueSchemaID); err != nil {
				return err
			}
			if k, err = o.GetEncoder(o.config.GetKey(), o.keyCodec, o.keySchemaID); err != nil {
				return err
			}
		} else {
			buf, err := json.Marshal(data)
			if err != nil {
				return err
			}
			k = sarama.StringEncoder(o.config.GetKey())
			v = sarama.StringEncoder(buf)
		}
	}

	msg := &sarama.ProducerMessage{
		Topic: o.config.GetTopic(),
		Key:   k,
		Value: v,
	}

	// _, _, err := o.producer.SendMessage(msg)
	o.producer.Input() <- msg
	return nil
}

func (o *DnstapKafkaOutput) close() {
	o.producer.AsyncClose()

	select {
	case <-o.errorLoggerClose:
	case <-time.After(3 * time.Second):
		log.Warn("Kafka output close timeout")
	}
}
