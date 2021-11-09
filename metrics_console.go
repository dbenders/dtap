package dtap

import (
	"time"

	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

type ConsoleExporter struct {
	interval time.Duration
}

func NewConsoleExporter(cfg *MetricsConsoleConfig) (*ConsoleExporter, error) {
	return &ConsoleExporter{
		interval: cfg.Interval,
	}, nil
}

func (e *ConsoleExporter) Start() {
	log.Info("start console metrics exporter")
	go metrics.Log(metrics.DefaultRegistry, e.interval, log.New())
}
