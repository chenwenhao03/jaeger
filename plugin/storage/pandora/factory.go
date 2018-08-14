// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pandora

import (
	"flag"

	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	//"github.com/jaegertracing/jaeger/pkg/es"
	//"github.com/jaegertracing/jaeger/pkg/es/config"
	depStore "github.com/jaegertracing/jaeger/plugin/storage/pandora/dependencystore"
	spanStore "github.com/jaegertracing/jaeger/plugin/storage/pandora/spanstore"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	"github.com/qiniu/pandora-go-sdk/pipeline"
)

// Factory implements storage.Factory for Pandora backend.
type Factory struct {
	Options *Options

	metricsFactory metrics.Factory
	logger         *zap.Logger

	primaryConfig *Configuration
	primaryClient pipeline.PipelineAPI
}

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	return &Factory{
		Options: NewOptions("pandora"), // TODO add "es-archive" once supported
	}
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.Options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.Options.InitFromViper(v)
	f.primaryConfig = f.Options.GetPrimary()
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	f.metricsFactory, f.logger = metricsFactory, logger

	//primaryClient, err := f.primaryConfig.NewClient(logger, metricsFactory)
	primaryClient, err := f.primaryConfig.NewClient()
	if err != nil {
		return err
	}
	f.primaryClient = primaryClient
	// TODO init archive (cf. https://github.com/jaegertracing/jaeger/pull/604)
	return nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	//cfg := f.primaryConfig
	return spanStore.NewSpanReader(f.primaryClient), nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	//cfg := f.primaryConfig
	return spanStore.NewSpanWriter(f.primaryClient), nil
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return depStore.NewDependencyStore(f.primaryClient, f.logger), nil
}
