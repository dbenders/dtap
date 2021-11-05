/*
 * Copyright (c) 2018 Manabu Sonoda
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
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	framestream "github.com/farsightsec/golang-framestream"
	strftime "github.com/jehiah/go-strftime"
	log "github.com/sirupsen/logrus"
)

type DnstapFstrmFileOutput struct {
	config          *OutputFileConfig
	currentFilename string
	enc             *framestream.Encoder
	writer          io.WriteCloser
	opened          chan bool
	mux             sync.Mutex
}

func NewDnstapFstrmFileOutput(config *OutputFileConfig, params *DnstapOutputParams) *DnstapOutput {
	params.Handler = &DnstapFstrmFileOutput{
		config: config,
	}
	return NewDnstapOutput(params)
}

func (o *DnstapFstrmFileOutput) open() error {
	filename := strftime.Format(o.config.GetPath(), time.Now())
	log.Debugf("open output file %s\n", filename)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("failed to create file %s err: %w", filename, err)
	}
	o.writer = f

	o.enc, err = framestream.NewEncoder(o.writer, &framestream.EncoderOptions{ContentType: dnstap.FSContentType, Bidirectional: false})
	if err != nil {
		return fmt.Errorf("failed to create framestream encorder %s err: %w", filename, err)
	}
	o.currentFilename = filename
	o.opened = make(chan bool)
	go func() {
		ticker := time.NewTicker(FlushTimeout)
	L:
		for {
			select {
			case <-o.opened:
				break L
			case <-ticker.C:
				if err := o.flush(); err != nil {
					break L
				}
				filename := strftime.Format(o.config.GetPath(), time.Now())
				if filename != o.currentFilename {
					o.close()
					break L
				}
			}
		}
		ticker.Stop()
	}()
	return nil
}

func (o *DnstapFstrmFileOutput) flush() error {
	o.mux.Lock()
	defer o.mux.Unlock()
	return o.enc.Flush()
}

func (o *DnstapFstrmFileOutput) write(frame []byte) error {
	o.mux.Lock()
	defer o.mux.Unlock()
	if _, err := o.enc.Write(frame); err != nil {
		return err
	}
	return nil
}

func (o *DnstapFstrmFileOutput) close() {
	o.mux.Lock()
	defer o.mux.Unlock()
	o.enc.Flush()
	o.enc.Close()
	o.writer.Close()
	close(o.opened)
}
