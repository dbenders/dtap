package main

import (
	"github.com/rcrowley/go-metrics"
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
