package main

import (
	"net"
	"time"

	graphite "github.com/cyberdelia/go-metrics-graphite"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

var (
	// The total number of input frames
	TotalRecvInputFrame = metrics.GetOrRegisterCounter("dtap_input_recv_frame_total", nil)

	// The total number of lost input frames from buffer
	TotalLostInputFrame = metrics.GetOrRegisterCounter("dtap_input_lost_frame_total", nil)

	// The total number of output frames
	TotalRecvOutputFrame = metrics.GetOrRegisterCounter("dtap_output_recv_frame_total", nil)

	// The total number of lost output frames from buffer
	TotalLostOutputFrame = metrics.GetOrRegisterCounter("dtap_output_lost_frame_total", nil)
)

func metricsExporter(address string, interval time.Duration) error {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return err
	}
	go graphite.Graphite(metrics.DefaultRegistry, interval, "metrics", addr)
	log.Info("start metrics exporter")
	return nil
}
