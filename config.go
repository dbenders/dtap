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
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/viper"
)

type Config struct {
	InputMsgBuffer      uint
	InputUnix           []*InputUnixSocketConfig
	InputFile           []*InputFileConfig
	InputTail           []*InputTailConfig
	InputTCP            []*InputTCPSocketConfig
	OutputUnix          []*OutputUnixSocketConfig
	OutputFile          []*OutputFileConfig
	OutputTCP           []*OutputTCPSocketConfig
	OutputFluent        []*OutputFluentConfig
	OutputKafka         []*OutputKafkaConfig
	OutputNats          []*OutputNatsConfig
	OutputPrometheus    []*OutputPrometheus
	OutputStdout        []*OutputStdoutConfig
	OutputElasticSearch []*OutputElasticSearchConfig
	MetricsGraphite     *MetricsGraphiteConfig
	MetricsConsole      *MetricsConsoleConfig
}

var (
	DefaultCounters = []OutputPrometheusMetrics{
		{
			Name:   "dtap_query_qtype_total",
			Help:   "Total number of queries with a given query type.",
			Labels: []string{"Qtype"},
		}, {
			Name:   "dtap_query_rcode_total",
			Help:   "Total number of queries with a given query type.",
			Labels: []string{"Rcode"},
		}, {
			Name:   "dtap_query_tc_bit_total",
			Help:   "Total number of queries with a given query tc bit.",
			Labels: []string{"TC"},
		}, {
			Name:   "dtap_query_ad_bit_total",
			Help:   "Total number of queries with a given query ad bit.",
			Labels: []string{"AD"},
		}, {
			Name:   "dtap_query_socket_protocol_total",
			Help:   "Total number of queries with a given query transport rotocol.",
			Labels: []string{"SocketProtocol"},
		}, {
			Name:   "dtap_query_socket_family_total",
			Help:   "Total number of queries with a given query IP Protocol.",
			Labels: []string{"SocketFamily"},
		}, {
			Name:           "dtap_query_tld_total",
			Help:           "Total number of queries with a given query tld.",
			Labels:         []string{"TopLevelDomainName"},
			ExpireInterval: 5,
			ExpireSec:      60,
		}, {
			Name:           "dtap_query_sld_total",
			Help:           "Total number of queries with a given query tld.",
			Labels:         []string{"TopLevelDomainName"},
			ExpireInterval: 5,
			ExpireSec:      60,
		},
	}
)

func (c *Config) Validate() []error {
	errs := []error{}
	if c.InputMsgBuffer < 128 {
		errs = append(errs, errors.New("InputMsgBuffer must not small 128"))
	}
	for n, i := range c.InputUnix {
		if err := i.Validate(); err != nil {
			err.configType = "InputUnix"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, i := range c.InputFile {
		if err := i.Validate(); err != nil {
			err.configType = "InputFile"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, i := range c.InputTCP {
		if err := i.Validate(); err != nil {
			err.configType = "InputTCP"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputUnix {
		if err := o.Validate(); err != nil {
			err.configType = "OutputUnix"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputFile {
		if err := o.Validate(); err != nil {
			err.configType = "OutputFile"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputTCP {
		if err := o.Validate(); err != nil {
			err.configType = "OutputTCP"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputFluent {
		if err := o.Validate(); err != nil {
			err.configType = "OutputFluent"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputKafka {
		if err := o.Validate(); err != nil {
			err.configType = "OutputKafka"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputNats {
		if err := o.Validate(); err != nil {
			err.configType = "OutputNats"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputPrometheus {
		if err := o.Validate(); err != nil {
			err.configType = "OutputPrometheus"
			err.no = n
			errs = append(errs, err)
		}
	}
	for n, o := range c.OutputElasticSearch {
		if err := o.Validate(); err != nil {
			err.configType = "OutputElasticSearch"
			err.no = n
			errs = append(errs, err)
		}
	}
	if c.MetricsGraphite != nil {
		if err := c.MetricsGraphite.Validate(); err != nil {
			err.configType = "MetricsGraphite"
			errs = append(errs, err)
		}
	}
	if c.MetricsConsole != nil {
		if err := c.MetricsConsole.Validate(); err != nil {
			err.configType = "MetricsConsole"
			errs = append(errs, err)
		}
	}

	return errs
}

type ValidationError struct {
	configType string
	no         int
	errors     []error
}

func (e *ValidationError) Error() string {
	var msg string
	for _, err := range e.errors {
		msg += fmt.Sprintf("%s[%d]: %s\n", e.configType, e.no, err.Error())
	}
	return msg
}

func (e *ValidationError) Add(err error) {
	e.errors = append(e.errors, err)
}

func (e *ValidationError) Err() *ValidationError {
	if len(e.errors) > 0 {
		return e
	}
	return nil
}

func NewValidationError() *ValidationError {
	return &ValidationError{
		errors: []error{},
	}
}
func NewConfigFromFile(filename string) (*Config, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return NewConfigFromReader(f)
}

func NewConfigFromReader(r io.Reader) (*Config, error) {
	c := &Config{}
	v := viper.New()
	v.SetConfigType("toml")
	v.SetDefault("InputMsgBuffer", 10000)

	if err := v.ReadConfig(r); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}
	if err := v.Unmarshal(c); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	return c, nil
}

type InputUnixSocketConfig struct {
	Path string
	User string
}

func (i *InputUnixSocketConfig) Validate() *ValidationError {
	err := NewValidationError()
	if i.Path == "" {
		err.Add(errors.New("Path must not be empty"))
	}
	return err.Err()
}

func (i *InputUnixSocketConfig) GetPath() string {
	return i.Path
}
func (i *InputUnixSocketConfig) GetUser() string {
	return i.User
}

type InputFileConfig struct {
	Path string
}

func (i *InputFileConfig) Validate() *ValidationError {
	err := NewValidationError()
	if i.Path == "" {
		err.Add(errors.New("Path must not be empty"))
	}
	return err.Err()
}

func (i *InputFileConfig) GetPath() string {
	return i.Path
}

type InputTailConfig struct {
	Path string
}

func (i *InputTailConfig) Validate() *ValidationError {
	err := NewValidationError()
	if i.Path == "" {
		err.Add(errors.New("Path must not be empty"))
	}
	return err.Err()
}

func (i *InputTailConfig) GetPath() string {
	return i.Path
}

type InputTCPSocketConfig struct {
	Address string
	Port    uint16
}

func (i *InputTCPSocketConfig) Validate() *ValidationError {
	err := NewValidationError()
	if i.Address == "" {
		err.Add(errors.New("Host must not be empty"))
	}
	return err.Err()
}

func (i *InputTCPSocketConfig) GetNet() string {
	address := i.Address
	port := i.Port
	if address == "" {
		address = "0.0.0.0"
	}
	if port == 0 {
		port = 10053
	}
	if strings.Contains(address, ":") {
		address = "[" + address + "]"
	}
	return address + ":" + strconv.Itoa(int(port))
}

type OutputUnixSocketConfig struct {
	Path   string
	Buffer OutputBufferConfig
}

func (o *OutputUnixSocketConfig) Validate() *ValidationError {
	err := NewValidationError()
	if o.Path == "" {
		err.Add(errors.New("Path must not be empty"))
	}
	return err.Err()
}

func (o *OutputUnixSocketConfig) GetPath() string {
	return o.Path
}

type OutputFileConfig struct {
	Path   string
	User   string
	Buffer OutputBufferConfig
}

func (o *OutputFileConfig) Validate() *ValidationError {
	err := NewValidationError()
	if o.Path == "" {
		err.Add(errors.New("Path must not be empty"))
	}
	return err.Err()
}

func (o *OutputFileConfig) GetPath() string {
	return o.Path
}
func (o *OutputFileConfig) GetUser() string {
	return o.User
}

type OutputTCPSocketConfig struct {
	Host    string
	Workers int
	Port    uint16
	Buffer  OutputBufferConfig
}

func (o *OutputTCPSocketConfig) Validate() *ValidationError {
	err := NewValidationError()
	if o.Host == "" {
		err.Add(errors.New("Host must not be empty"))
	}
	if o.Workers == 0 {
		o.Workers = 1
	}
	return err.Err()
}

func (o *OutputTCPSocketConfig) GetAddress() string {
	host := o.Host
	port := o.Port
	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 10053
	}
	return host + ":" + strconv.Itoa(int(port))
}

type OutputFluentConfig struct {
	Host   string
	Tag    string
	Port   uint16
	Flat   FlatConfig
	Buffer OutputBufferConfig
}

func (o *OutputFluentConfig) Validate() *ValidationError {
	valerr := NewValidationError()
	if o.Host == "" {
		valerr.Add(errors.New("Host must not be empty"))
	}
	if o.Tag == "" {
		valerr.Add(errors.New("Tag must not be empty"))
	} else {
		r := regexp.MustCompile(`^[a-z0-9_]+$`)
		labels := strings.Split(o.Tag, ".")
		for _, label := range labels {
			if r.MatchString(label) {
				valerr.Add(errors.New("Tag characters must only include lower-case alphabets, digits underscore and dot"))
				break
			}
		}
		if o.Tag[0] == '.' {
			valerr.Add(errors.New("First part of a tag is empty"))
		}
		if o.Tag[len(o.Tag)-1] == '.' {
			valerr.Add(errors.New("Last part of a tag is empty"))
		}
	}
	if err := o.Flat.Validate(); err != nil {
		valerr.Add(err)
	}
	return valerr.Err()
}

func (o *OutputFluentConfig) GetHost() string {
	return o.Host
}

func (o *OutputFluentConfig) GetTag() string {
	return o.Tag
}

func (o *OutputFluentConfig) GetPort() int {
	if o.Port == 0 {
		return 24224
	}
	return int(o.Port)
}

type OutputKafkaConfig struct {
	Hosts            []string
	SchemaRegistries []string
	Retry            uint
	Topic            string
	Key              string
	OutputType       string
	Buffer           OutputBufferConfig
	Flat             FlatConfig
	Metrics          bool
	Workers          int
}

func (o *OutputKafkaConfig) Validate() *ValidationError {
	valerr := NewValidationError()
	if o.Topic == "" {
		valerr.Add(errors.New("Topic must not be empty"))
	}
	if len(o.Hosts) == 0 {
		valerr.Add(errors.New("Hosts must not be empty"))
	}
	otype := strings.ToLower(o.OutputType)
	switch otype {
	case "json", "flat_json":
	case "protobuf":
	case "avro":
	default:
		valerr.Add(errors.New("OutputType must be avro, json or protobuf"))
	}
	o.OutputType = otype
	if o.Workers == 0 {
		o.Workers = 1
	}
	return valerr.Err()
}

func (o *OutputKafkaConfig) GetHosts() []string {
	return o.Hosts
}
func (o *OutputKafkaConfig) GetSchemaRegistries() []string {
	return o.SchemaRegistries
}
func (o *OutputKafkaConfig) GetRetry() uint {
	return o.Retry
}
func (o *OutputKafkaConfig) GetTopic() string {
	return o.Topic
}
func (o *OutputKafkaConfig) GetKey() string {
	return o.Key
}
func (o *OutputKafkaConfig) GetOutputType() string {
	if o.OutputType == "" {
		return "avro"
	}
	return o.OutputType
}

type OutputNatsConfig struct {
	Host     string
	Subject  string
	User     string
	Password string
	Token    string
	Flat     FlatConfig
	Buffer   OutputBufferConfig
}

func (o *OutputNatsConfig) Validate() *ValidationError {
	valerr := NewValidationError()
	if err := o.Flat.Validate(); err != nil {
		valerr.Add(err)
	}

	return valerr.Err()
}

func (o *OutputNatsConfig) GetHost() string {
	return o.Host
}
func (o *OutputNatsConfig) GetSubject() string {
	return o.Subject
}
func (o *OutputNatsConfig) GetUser() string {
	return o.User
}
func (o *OutputNatsConfig) GetPassword() string {
	return o.Password
}
func (o *OutputNatsConfig) GetToken() string {
	return o.Token
}

type OutputPrometheus struct {
	Counters []OutputPrometheusMetrics
	Flat     FlatConfig
	Buffer   OutputBufferConfig
}

func (o *OutputPrometheus) GetCounters() []OutputPrometheusMetrics {
	if o.Counters == nil {
		return DefaultCounters
	}
	return o.Counters
}

func (o *OutputPrometheus) Validate() *ValidationError {
	return nil
}

type OutputPrometheusMetrics struct {
	Name           string
	Help           string
	Labels         []string
	Limit          int
	ExpireInterval int
	ExpireSec      int
}

func (o *OutputPrometheusMetrics) GetName() string {
	return o.Name
}

func (o *OutputPrometheusMetrics) GetHelp() string {
	return o.Help
}

func (o *OutputPrometheusMetrics) GetLabels() []string {
	return o.Labels
}

func (o *OutputPrometheusMetrics) GetLimit() int {
	return o.Limit
}

func (o *OutputPrometheusMetrics) GetExpireInterval() int {
	return o.ExpireInterval
}

func (o *OutputPrometheusMetrics) GetExpireSec() int {
	return o.ExpireSec
}

type OutputStdoutConfig struct {
	Type        string             `toml:"type"`
	TemplateStr string             `toml:"template"`
	template    *template.Template `toml:"-"`
	Flat        FlatConfig
	Buffer      OutputBufferConfig
}

func (o *OutputStdoutConfig) GetType() string {
	if o.Type == "" {
		return "json"
	}
	return o.Type
}
func (o *OutputStdoutConfig) Validate() error {
	valerr := NewValidationError()
	o.Type = strings.ToLower(o.Type)
	switch o.Type {
	case "", "json":
		o.Type = "json"
	case "gotpl":
		if o.TemplateStr == "" {
			valerr.Add(errors.New("Type gotpl need Template string"))
		}
		t, err := template.New("stdout").Parse(o.TemplateStr)
		if err != nil {
			valerr.Add(fmt.Errorf("Template parse error: %w", err))
		}
		o.template = t
	default:
		valerr.Add(errors.New("Type must be json or gotpl"))
	}
	if err := o.Flat.Validate(); err != nil {
		valerr.Add(err)
	}
	return valerr.Err()
}

type OutputBufferConfig struct {
	BufferSize uint
}

func (o *OutputBufferConfig) GetBufferSize() uint {
	if o.BufferSize == 0 {
		return OutputBufferSize
	}
	return o.BufferSize
}

type FlatConfig struct {
	IPv4Mask       uint8
	ipv4Mask       net.IPMask
	IPv6Mask       uint8
	ipv6Mask       net.IPMask
	EnableECS      bool
	EnableHashIP   bool
	ipHashSalt     []byte `toml:"-"`
	IPHashSaltPath string
	saltMutex      sync.RWMutex
}

func (o *FlatConfig) GetIPv4Mask() net.IPMask {
	return o.ipv4Mask
}

func (o *FlatConfig) GetIPv6Mask() net.IPMask {
	return o.ipv6Mask
}

func (o *FlatConfig) GetEnableEcs() bool {
	return o.EnableECS
}

func (o *FlatConfig) GetEnableHashIP() bool {
	return o.EnableHashIP
}

func (o *FlatConfig) GetIPHashSaltPath() string {
	return o.IPHashSaltPath
}

func (o *FlatConfig) GetIPHashSalt() []byte {
	o.saltMutex.RLock()
	defer o.saltMutex.RUnlock()
	return o.ipHashSalt
}

func (o *FlatConfig) LoadSalt() {
	o.saltMutex.Lock()
	defer o.saltMutex.Unlock()
	if o.IPHashSaltPath != "" {
		o.ipHashSalt, _ = ioutil.ReadFile(o.GetIPHashSaltPath())
	}
}

func (o *FlatConfig) WatchSalt(ctx context.Context, ready chan struct{}) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()
	err = watcher.Add(o.IPHashSaltPath)
	if err != nil {
		log.Fatal(err)
	}
	close(ready)
L:
	for {
		select {
		case <-ctx.Done():
			break L
		case event, ok := <-watcher.Events:
			if !ok {
				break L
			}
			log.Info("event:", event)
			o.LoadSalt()
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Info("error:", err)
		}
	}
}

func (o *FlatConfig) Validate() *ValidationError {
	valerr := NewValidationError()
	if o.IPv4Mask != 0 {
		if o.IPv4Mask > 32 {
			valerr.Add(errors.New("IPv4Mask must include range 0 to 32"))
		}
	} else {
		o.IPv4Mask = 24
	}
	o.ipv4Mask = net.CIDRMask(int(o.IPv4Mask), 32)

	if o.IPv6Mask != 0 {
		if o.IPv6Mask > 128 {
			valerr.Add(errors.New("IPv4Mask must include range 0 to 128"))
		}
	} else {
		o.IPv6Mask = 48
	}
	o.ipv6Mask = net.CIDRMask(int(o.IPv6Mask), 128)

	if o.ipHashSalt == nil {
		if o.IPHashSaltPath != "" {
			o.LoadSalt()
		}
	}
	if o.ipHashSalt == nil {
		o.ipHashSalt = make([]byte, 32)
		rand.Read(o.ipHashSalt)
	}

	return valerr.Err()
}

type MetricsGraphiteConfig struct {
	Address   string
	Interval  time.Duration
	Namespace string
}

type MetricsConsoleConfig struct {
	Interval time.Duration
}

func (c *MetricsGraphiteConfig) Validate() *ValidationError {
	valerr := NewValidationError()
	if _, err := net.ResolveTCPAddr("tcp", c.Address); err != nil {
		valerr.Add(err)
	}
	if c.Interval == 0 {
		valerr.Add(errors.New("interval cannot be zero"))
	}

	if c.Namespace == "" {
		c.Namespace = "dtap"
	}

	return valerr.Err()
}

func (c *MetricsConsoleConfig) Validate() *ValidationError {
	valerr := NewValidationError()
	if c.Interval == 0 {
		valerr.Add(errors.New("interval cannot be zero"))
	}
	return valerr.Err()
}

type OutputElasticSearchConfig struct {
	Flat          FlatConfig
	Buffer        OutputBufferConfig
	Addresses     []string
	Username      string
	Password      string
	Index         string
	Workers       int           // The number of workers. Defaults to runtime.NumCPU().
	FlushBytes    int           // The flush threshold in bytes. Defaults to 5MB.
	FlushInterval time.Duration // The flush threshold as duration. Defaults to 30sec.
}

func (c *OutputElasticSearchConfig) Validate() *ValidationError {
	valerr := NewValidationError()
	if len(c.Addresses) == 0 {
		valerr.Add(errors.New("Addresses must not be empty"))
	}
	if c.Index == "" {
		valerr.Add(errors.New("IndexName must not be empty"))
	}
	if c.Workers == 0 {
		c.Workers = 1
	}
	if c.FlushBytes == 0 {
		c.FlushBytes = 5 * 1024 * 1024
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 30 * time.Second
	}
	return valerr.Err()
}
