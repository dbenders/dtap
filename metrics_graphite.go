package dtap

import (
	"net"
	"time"

	graphite "github.com/cyberdelia/go-metrics-graphite"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

type GraphiteExporter struct {
	address  *net.TCPAddr
	interval time.Duration
}

func NewGraphiteExporter(cfg *MetricsGraphiteConfig) (*GraphiteExporter, error) {
	addr, err := net.ResolveTCPAddr("tcp", cfg.Address)
	if err != nil {
		return nil, err
	}
	return &GraphiteExporter{
		address:  addr,
		interval: cfg.Interval,
	}, nil
}

func (e *GraphiteExporter) Start() {
	log.Info("start graphite metrics exporter")
	go graphite.Graphite(metrics.DefaultRegistry, e.interval, "metrics", e.address)
}
