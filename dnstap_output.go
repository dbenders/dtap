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
	"context"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

type DnstapOutputParams struct {
	BufferSize  uint
	InCounter   metrics.Counter
	LostCounter metrics.Counter
	Handler     OutputHandler
}

type DnstapOutput struct {
	handler OutputHandler
	rbuf    *RBuf
}

func NewDnstapOutput(params *DnstapOutputParams) *DnstapOutput {
	return &DnstapOutput{
		handler: params.Handler,
		rbuf:    NewRbuf(params.BufferSize, params.InCounter, params.LostCounter),
	}
}

func (o *DnstapOutput) Run(ctx context.Context) {
	log.Debug("start output run")
L:
	for {
		select {
		case <-ctx.Done():
			log.Debug("Run ctx done")
			break L
		default:
			if err := o.handler.open(); err != nil {
				log.Debug(err)
				// wait before retrying
				time.Sleep(1 * time.Second)
				continue
			}
			log.Debug("success open")
			err := o.run(ctx)
			log.Debug("close handle close")
			o.handler.close()

			if err != nil {
				log.Debug(err)
			} else {
				break L
			}
		}
	}
	return
}
func (o *DnstapOutput) run(ctx context.Context) error {
	log.Debug("start writer")
L:
	for {
		select {
		case <-ctx.Done():
			break L
		case frame := <-o.rbuf.Read():
			if frame != nil {
				if err := o.handler.write(frame); err != nil {
					log.Debugf("writer error: %v", err)
					return err
				}
			}
		}
	}
	log.Debug("end writer")
	return nil
}

func (o *DnstapOutput) SetMessage(b []byte) {
	o.rbuf.Write(b)
}

type DnstapMultiWorkerOutput struct {
	outputs []Output
	curr    int
}

func NewDnstapMultiWorkerOutput(outs []Output) *DnstapMultiWorkerOutput {
	return &DnstapMultiWorkerOutput{outputs: outs}
}

func (o *DnstapMultiWorkerOutput) Run(ctx context.Context) {
	if len(o.outputs) == 1 {
		o.outputs[0].Run(ctx)
		return
	}

	var wg sync.WaitGroup
	for _, out := range o.outputs {
		wg.Add(1)
		go func(out Output) {
			defer wg.Done()
			out.Run(ctx)
		}(out)
	}
	log.Info("Wait for all workers")
	wg.Wait()
	log.Info("All workers done")
}

func (o *DnstapMultiWorkerOutput) SetMessage(msg []byte) {
	o.outputs[o.curr].SetMessage(msg)
	o.curr = (o.curr + 1) % len(o.outputs)
}
