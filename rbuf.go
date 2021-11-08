package dtap

import (
	"sync"

	"github.com/rcrowley/go-metrics"
)

type RBuf struct {
	channel     chan []byte
	mux         sync.Mutex
	inCounter   metrics.Counter
	lostCounter metrics.Counter
}

func NewRbuf(size uint, inCounter metrics.Counter, lostCounter metrics.Counter) *RBuf {
	rbuf := &RBuf{
		channel:     make(chan []byte, size),
		mux:         sync.Mutex{},
		inCounter:   inCounter,
		lostCounter: lostCounter,
	}
	return rbuf
}

func (r *RBuf) Read() <-chan []byte {
	return r.channel
}

func (r *RBuf) Write(b []byte) {
	r.mux.Lock()
	select {
	case r.channel <- b:
		r.inCounter.Inc(1)
	default:
		r.lostCounter.Inc(1)
		r.inCounter.Inc(1)
		<-r.channel
		r.channel <- b
	}
	r.mux.Unlock()
}

func (r *RBuf) Close() {
	close(r.channel)
}
