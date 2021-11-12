package main

import (
	"compress/gzip"
	"flag"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	framestream "github.com/farsightsec/golang-framestream"
	log "github.com/sirupsen/logrus"
)

var (
	flagFilePath   = flag.String("i", "dnstap.fstrm.gz", "input file path (.gz)")
	flagAddress    = flag.String("o", "localhost:11111", "output address (host:port)")
	flagTargetRate = flag.Int("r", 10000, "target rate of req/s")
	flagNum        = flag.Int("n", 99999, "number of messages")
)

func initReader() (*framestream.Decoder, io.ReadCloser) {
	f, err := os.Open(*flagFilePath)
	if err != nil {
		log.Fatal(err)
	}
	var r io.ReadCloser
	r, err = gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	dec, err := framestream.NewDecoder(r, &framestream.DecoderOptions{
		ContentType:   dnstap.FSContentType,
		Bidirectional: false,
	})
	if err != nil {
		log.Fatal(err)
	}
	return dec, r
}

func main() {
	flag.Parse()

	count := 0

	lock := sync.Mutex{}
	cond := sync.NewCond(&lock)

	w, err := net.Dial("tcp", *flagAddress)
	if err != nil {
		log.Fatal(err)
	}
	w.SetWriteDeadline(time.Time{})

	enc, err := framestream.NewEncoder(w, &framestream.EncoderOptions{ContentType: dnstap.FSContentType, Bidirectional: true})
	if err != nil {
		log.Fatal(err)
	}

	var rate uint32

	go func() {
		tick := time.Tick(1 * time.Second)
		for range tick {
			log.Infof("%d req/s", atomic.LoadUint32(&rate))
			atomic.StoreUint32(&rate, 0)
			lock.Lock()
			cond.Signal()
			lock.Unlock()
		}
	}()

	for {
		dec, r := initReader()
		for {
			buf, err := dec.Decode()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Error(err)
			}
			_rate := atomic.AddUint32(&rate, 1)
			count++
			if count >= *flagNum {
				r.Close()
				enc.Close()
				return
			}
			if int(_rate) > *flagTargetRate {
				cond.L.Lock()
				cond.Wait()
				cond.L.Unlock()
			}

			n, err := enc.Write(buf)
			if n < len(buf) {
				log.Errorf("write less (%d of %d)", n, len(buf))
			}
			if err != nil {
				log.Error(err)
			}
		}
	}
}
