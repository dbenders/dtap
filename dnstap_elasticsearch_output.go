package dtap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type DnstapElasticSearchOutput struct {
	config     *OutputElasticSearchConfig
	flatOption DnstapFlatOption
	esConfig   *elasticsearch.Config
	client     *elasticsearch.Client
	indexer    esutil.BulkIndexer
}

type logger struct{}

func (l logger) Printf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Debug(msg)
}

func NewDnstapElasticSearchOutput(config *OutputElasticSearchConfig, params *DnstapOutputParams) *DnstapOutput {
	esConfig := elasticsearch.Config{
		Addresses: config.Addresses,
		Username:  config.Username,
		Password:  config.Password,
	}

	// TODO: retry

	params.Handler = &DnstapElasticSearchOutput{
		config:     config,
		flatOption: &config.Flat,
		esConfig:   &esConfig,
	}

	return NewDnstapOutput(params)
}

func (o *DnstapElasticSearchOutput) open() error {
	var err error
	o.client, err = elasticsearch.NewClient(*o.esConfig)
	if err != nil {
		return fmt.Errorf("failed to open elasticsearch client: %w", err)
	}

	o.indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         o.config.Index,
		Client:        o.client,
		NumWorkers:    o.config.Workers,
		FlushBytes:    int(o.config.FlushBytes),
		FlushInterval: o.config.FlushInterval,
		DebugLogger:   logger{},
		OnError:       o.OnError,
	})
	if err != nil {
		return fmt.Errorf("failed to create bulk indexer: %w", err)
	}

	return nil
}

var ctx = context.Background()

func (o *DnstapElasticSearchOutput) write(frame []byte) error {
	if o.indexer == nil {
		return fmt.Errorf("elasticsearch indexer is not initialized")
	}

	// reencode from protobuf to json
	dt := dnstap.Dnstap{}
	if err := proto.Unmarshal(frame, &dt); err != nil {
		return err
	}
	data, err := FlatDnstap(&dt, o.flatOption)
	if err != nil {
		return err
	}
	buf, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	item := esutil.BulkIndexerItem{
		Index:     o.config.Index,
		Action:    "index",
		Body:      bytes.NewReader(buf),
		OnSuccess: nil,
		OnFailure: nil,
	}
	err = o.indexer.Add(ctx, item)
	if err != nil {
		return fmt.Errorf("failed to add item to bulk indexer: %w", err)
	}

	return nil
}

func (o *DnstapElasticSearchOutput) OnError(ctx context.Context, err error) {
	log.Errorf("failed to index item: %v", err)
}

func (o *DnstapElasticSearchOutput) marshal(flatdt *DnstapFlatT) ([]byte, error) {

func (o *DnstapElasticSearchOutput) close() {
	err := o.indexer.Close(ctx)
	if err != nil {
		log.Errorf("failed to close bulk indexer: %v", err)
	}
}
