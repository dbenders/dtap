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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"

	// _ "net/http/pprof"

	"github.com/mimuret/dtap"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetOutput(os.Stderr)
	log.SetLevel(log.InfoLevel)
}

var (
	flagConfigFile = flag.String("c", "dtap.toml", "config file path")
	flagLogLevel   = flag.String("d", "info", "log level(debug,info,warn,error,fatal)")
	flagCPUProfile = flag.String("cpuprofile", "", "CPU Profiling output file")
	flagMemProfile = flag.String("memprofile", "", "Memory Profiling output file")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]...\n", os.Args[0])
	flag.PrintDefaults()
}

func outputLoop(output []dtap.Output, irbuf *dtap.RBuf) {
	log.Info("start outputLoop")
	for frame := range irbuf.Read() {
		for _, o := range output {
			o.SetMessage(frame)
		}
	}
	log.Info("finish outputLoop")
}

func fatalCheck(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func main() {
	var err error
	// runtime.SetMutexProfileFraction(1)
	// runtime.SetBlockProfileRate(1)

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Usage = usage

	flag.Parse()

	if *flagCPUProfile != "" {
		f, err := os.Create(*flagCPUProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// set log level
	switch *flagLogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	default:
		usage()
		os.Exit(1)
	}
	var input []dtap.Input
	var output []dtap.Output
	var metricExporters []dtap.MetricsExporter

	config, err := dtap.NewConfigFromFile(*flagConfigFile)
	fatalCheck(err)

	if errs := config.Validate(); len(errs) > 0 {
		for _, err := range errs {
			log.Error(err)
		}
		log.Fatal("Error reading config file")
	}

	if config.MetricsGraphite != nil {
		exp, err := dtap.NewGraphiteExporter(config.MetricsGraphite)
		fatalCheck(err)
		metricExporters = append(metricExporters, exp)
	}
	if config.MetricsConsole != nil {
		exp, err := dtap.NewConsoleExporter(config.MetricsConsole)
		fatalCheck(err)
		metricExporters = append(metricExporters, exp)
	}

	for _, ic := range config.InputFile {
		i, err := dtap.NewDnstapFstrmFileInput(ic)
		fatalCheck(err)
		input = append(input, i)
	}

	for _, ic := range config.InputTCP {
		i, err := dtap.NewDnstapFstrmTCPSocketInput(ic)
		fatalCheck(err)
		input = append(input, i)
	}

	for _, ic := range config.InputUnix {
		i, err := dtap.NewDnstapFstrmUnixSocketInput(ic)
		fatalCheck(err)
		input = append(input, i)
	}

	if len(input) == 0 {
		log.Fatal("No input settings")
	}

	for _, oc := range config.OutputFile {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		o := dtap.NewDnstapFstrmFileOutput(oc, params)
		output = append(output, o)
	}

	for _, oc := range config.OutputTCP {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		o := dtap.NewDnstapFstrmTCPSocketOutput(oc, params)
		output = append(output, o)
	}

	for _, oc := range config.OutputUnix {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		o := dtap.NewDnstapFstrmUnixSockOutput(oc, params)
		output = append(output, o)
	}

	for _, oc := range config.OutputFluent {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		o := dtap.NewDnstapFluentdOutput(oc, params)
		output = append(output, o)
		if oc.Flat.GetIPHashSaltPath() != "" {
			ready := make(chan struct{})
			go oc.Flat.WatchSalt(context.Background(), ready)
			log.Info("wait for ready to watch salt files")
			<-ready
		}
	}

	for _, oc := range config.OutputKafka {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		var o dtap.Output
		if oc.Workers > 1 {
			outs := make([]dtap.Output, oc.Workers)
			for i := 0; i < oc.Workers; i++ {
				outs[i], err = dtap.NewDnstapKafkaOutput(oc, params)
				fatalCheck(err)
			}
			o = dtap.NewDnstapMultiWorkerOutput(outs)
		} else {
			o, err = dtap.NewDnstapKafkaOutput(oc, params)
			fatalCheck(err)
		}
		output = append(output, o)
	}

	for _, oc := range config.OutputNats {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		o := dtap.NewDnstapNatsOutput(oc, params)
		output = append(output, o)
		if oc.Flat.GetIPHashSaltPath() != "" {
			ready := make(chan struct{})
			go oc.Flat.WatchSalt(context.Background(), ready)
			log.Info("wait for ready to watch salt files")
			<-ready
		}
	}

	for _, oc := range config.OutputPrometheus {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		o := dtap.NewDnstapPrometheusOutput(oc, params)
		output = append(output, o)
	}
	for _, oc := range config.OutputStdout {
		params := &dtap.DnstapOutputParams{
			BufferSize:  oc.Buffer.GetBufferSize(),
			InCounter:   TotalRecvOutputFrame,
			LostCounter: TotalLostOutputFrame,
		}
		o := dtap.NewDnstapStdoutOutput(oc, params)
		output = append(output, o)
	}

	if len(output) == 0 {
		log.Fatal("No output settings")
	}

	for _, exp := range metricExporters {
		exp.Start()
	}

	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	iRBuf := dtap.NewRbuf(config.InputMsgBuffer, TotalRecvInputFrame, TotalLostInputFrame)
	fatalCh := make(chan error)

	outputCtx, outputCancel := context.WithCancel(context.Background())
	owg := &sync.WaitGroup{}
	for _, o := range output {
		owg.Add(1)
		go func(o dtap.Output) {
			o.Run(outputCtx)
			owg.Done()
		}(o)
	}
	go outputLoop(output, iRBuf)

	inputCtx, intputCancel := context.WithCancel(context.Background())

	iwg := &sync.WaitGroup{}
	for _, i := range input {
		iwg.Add(1)
		go func(i dtap.Input) {
			err := i.Run(inputCtx, iRBuf)
			if err != nil {
				log.Error(err)
				fatalCh <- err
			}
			iwg.Done()
		}(i)
	}
	inputFinish := make(chan struct{})
	go func() {
		iwg.Wait()
		close(inputFinish)
	}()

	log.Info("finish boot dtap")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)

	select {
	case <-sigCh:
		log.Info("recieve signal")
	case err := <-fatalCh:
		log.Error(err)
	case <-inputFinish:
		log.Debug("finish all input task")
	}

	log.Info("wait finish input task")
	intputCancel()
	iwg.Wait()
	log.Info("done")

	log.Info("wait finish output task")
	outputCancel()
	owg.Wait()
	log.Info("done")

	iRBuf.Close()

	if *flagMemProfile != "" {
		f, err := os.Create(*flagMemProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	//os.Exit(0)
}
