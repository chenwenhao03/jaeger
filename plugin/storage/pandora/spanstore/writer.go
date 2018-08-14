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

package spanstore

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
	"gopkg.in/olivere/elastic.v5"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/model/converter/json"
	jModel "github.com/jaegertracing/jaeger/model/json"
	"github.com/jaegertracing/jaeger/pkg/cache"
	//"github.com/jaegertracing/jaeger/pkg/es"
	storageMetrics "github.com/jaegertracing/jaeger/storage/spanstore/metrics"


	"github.com/qiniu/pandora-go-sdk/pipeline"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const (
	spanType    = "span"
	serviceType = "service"

	defaultNumShards = 5
)

type spanWriterMetrics struct {
	indexCreate *storageMetrics.WriteMetrics
}

type serviceWriter func(string, *jModel.Span)

// SpanWriter is a wrapper around elastic.Client
type SpanWriter struct {
	ctx           context.Context
	client        pipeline.PipelineAPI
	logger        *zap.Logger
	writerMetrics spanWriterMetrics // TODO: build functions to wrap around each Do fn
	indexCache    cache.Cache
	serviceWriter serviceWriter
	//numShards     int64
	//numReplicas   int64
}
var _ spanstore.Writer = &SpanWriter{}

// Service is the JSON struct for service:operation documents in ElasticSearch
type Service struct {
	ServiceName   string `json:"serviceName"`
	OperationName string `json:"operationName"`
}

// Span adds a StartTimeMillis field to the standard JSON span.
// ElasticSearch does not support a UNIX Epoch timestamp in microseconds,
// so Jaeger maps StartTime to a 'long' type. This extra StartTimeMillis field
// works around this issue, enabling timerange queries.
type Span struct {
	*jModel.Span
	StartTimeMillis uint64 `json:"startTimeMillis"`
}

func (s Service) hashCode() string {
	h := fnv.New64a()
	h.Write([]byte(s.ServiceName))
	h.Write([]byte(s.OperationName))
	return fmt.Sprintf("%x", h.Sum64())
}

// NewSpanWriter creates a new SpanWriter for use
func NewSpanWriter(client pipeline.PipelineAPI) *SpanWriter {
	ctx := context.Background()
	//if numShards == 0 {
	//	numShards = defaultNumShards
	//}

	// TODO: Configurable TTL
	serviceOperationStorage := NewServiceOperationStorage(ctx, client, time.Hour*12)
	return &SpanWriter{
		ctx:    ctx,
		client: client,
		//logger: logger,
		//writerMetrics: spanWriterMetrics{
		//	indexCreate: storageMetrics.NewWriteMetrics(metricsFactory, "index_create"),
		//},
		serviceWriter: serviceOperationStorage.Write,
		indexCache: cache.NewLRUWithOptions(
			5,
			&cache.Options{
				TTL: 48 * time.Hour,
			},
		),
		//numShards:   numShards,
		//numReplicas: numReplicas,
	}
}

// WriteSpan writes a span and its corresponding service:operation in ElasticSearch
func (s *SpanWriter) WriteSpan(ctx context.Context,span *model.Span) error {
	//spanIndexName, serviceIndexName := indexNames(span)
	//// Convert model.Span into json.Span
	//jsonSpan := json.FromDomainEmbedProcess(span)
	//
	//if err := s.createIndex(serviceIndexName, serviceMapping, jsonSpan); err != nil {
	//	return err
	//}
	//s.writeService(serviceIndexName, jsonSpan)
	//if err := s.createIndex(spanIndexName, spanMapping, jsonSpan); err != nil {
	//	return err
	//}
	//s.writeSpan(spanIndexName, jsonSpan)
	var points pipeline.Datas
	//now := time.Now().Format(time.RFC3339Nano)
	// Convert model.Span into json.Span
	jsonSpan := json.FromDomainEmbedProcess(span)
	//for _, d := range datas {
		//if d == nil {
		//	continue
		//}
		//if s.opt.logkitSendTime {
		//	d[KeyLogkitSendTime] = now
		//}
		//if s.opt.extraInfo {
		//	for key, val := range s.extraInfo {
		//		if _, exist := d[key]; !exist {
		//			d[key] = val
		//		}
		//	}
		//}
		//point := s.generatePoint(d)
		points = append(points, jsonSpan)
	//}


	data := pipeline.PostDataInput{
		Points:points,
	}
	points
	return nil
}

// Close closes SpanWriter
func (s *SpanWriter) Close() error {
	return s.client.Close()
}

func indexNames(span *model.Span) (string, string) {
	spanDate := span.StartTime.UTC().Format("2006-01-02")
	return spanIndexPrefix + spanDate, serviceIndexPrefix + spanDate
}

func (s *SpanWriter) createIndex(indexName string, mapping string, jsonSpan *jModel.Span) error {
	if !keyInCache(indexName, s.indexCache) {
		start := time.Now()
		exists, _ := s.client.IndexExists(indexName).Do(s.ctx) // don't need to check the error because the exists variable will be false anyway if there is an error
		if !exists {
			// if there are multiple collectors writing to the same elasticsearch host a race condition can occur - create the index multiple times
			// we check for the error type to minimize errors
			_, err := s.client.CreateIndex(indexName).Body(s.fixMapping(mapping)).Do(s.ctx)
			s.writerMetrics.indexCreate.Emit(err, time.Since(start))
			if err != nil {
				eErr, ok := err.(*elastic.Error)
				if !ok || eErr.Details != nil &&
					// ES 5.x
					(eErr.Details.Type != "index_already_exists_exception" &&
						// ES 6.x
						eErr.Details.Type != "resource_already_exists_exception") {
					return s.logError(jsonSpan, err, "Failed to create index", s.logger)
				}
			}
		}
		writeCache(indexName, s.indexCache)
	}
	return nil
}

func keyInCache(key string, c cache.Cache) bool {
	return c.Get(key) != nil
}

func writeCache(key string, c cache.Cache) {
	c.Put(key, key)
}

func (s *SpanWriter) fixMapping(mapping string) string {
	mapping = strings.Replace(mapping, "${__NUMBER_OF_SHARDS__}", strconv.FormatInt(s.numShards, 10), 1)
	mapping = strings.Replace(mapping, "${__NUMBER_OF_REPLICAS__}", strconv.FormatInt(s.numReplicas, 10), 1)
	return mapping
}

func (s *SpanWriter) writeService(indexName string, jsonSpan *jModel.Span) {
	s.serviceWriter(indexName, jsonSpan)
}

func (s *SpanWriter) writeSpan(indexName string, jsonSpan *jModel.Span) {
	elasticSpan := Span{Span: jsonSpan, StartTimeMillis: jsonSpan.StartTime / 1000} // Microseconds to milliseconds

	s.client.Index().Index(indexName).Type(spanType).BodyJson(&elasticSpan).Add()
}

func (s *SpanWriter) logError(span *jModel.Span, err error, msg string, logger *zap.Logger) error {
	logger.
		With(zap.String("trace_id", string(span.TraceID))).
		With(zap.String("span_id", string(span.SpanID))).
		With(zap.Error(err)).
		Error(msg)
	return errors.Wrap(err, msg)
}
// Data store as use key/value map
type Data map[string]interface{}
func repoName(prefix string,span *model.SpanWrapper)string{
	return prefix + span.UID

}
func (s *SpanWriter) Send(datas []Data) (se error) {
	//s.checkSchemaUpdate()
	if !s.opt.schemaFree && (len(s.schemas) <= 0 || len(s.alias2key) <= 0) {
		se = reqerr.NewSendError("Get pandora schema error, failed to send data", ConvertDatasBack(datas), reqerr.TypeDefault)
		ste := &StatsError{
			StatsInfo: StatsInfo{
				Success:   0,
				Errors:    int64(len(datas)),
				LastError: "Get pandora schema error or repo not exist",
			},
			ErrorDetail: se,
		}
		return ste
	}
	var points pipeline.Datas
	now := time.Now().Format(time.RFC3339Nano)
	for _, d := range datas {
		if d == nil {
			continue
		}
		if s.opt.logkitSendTime {
			d[KeyLogkitSendTime] = now
		}
		if s.opt.extraInfo {
			for key, val := range s.extraInfo {
				if _, exist := d[key]; !exist {
					d[key] = val
				}
			}
		}
		point := s.generatePoint(d)
		points = append(points, pipeline.Data(map[string]interface{}(point)))
	}
	s.opt.tokenLock.RLock()
	schemaFreeInput := &pipeline.SchemaFreeInput{
		WorkflowName:    s.opt.workflowName,
		RepoName:        s.opt.repoName,
		NoUpdate:        !s.opt.schemaFree,
		Datas:           points,
		SchemaFreeToken: s.opt.tokens.SchemaFreeTokens,
		RepoOptions:     &pipeline.RepoOptions{WithIP: s.opt.withip, UnescapeLine: s.opt.UnescapeLine},
		Option: &pipeline.SchemaFreeOption{
			NumberUseFloat: s.opt.numberUseFloat,
			ToLogDB:        s.opt.enableLogdb,
			AutoExportToLogDBInput: pipeline.AutoExportToLogDBInput{
				OmitEmpty:             true,
				OmitInvalid:           false,
				RepoName:              s.opt.repoName,
				LogRepoName:           s.opt.logdbReponame,
				AnalyzerInfo:          s.opt.analyzerInfo,
				AutoExportLogDBTokens: s.opt.tokens.LogDBTokens,
			},
			ToKODO: s.opt.enableKodo,
			AutoExportToKODOInput: pipeline.AutoExportToKODOInput{
				Retention:            30,
				RepoName:             s.opt.repoName,
				BucketName:           s.opt.bucketName,
				Email:                s.opt.email,
				Prefix:               s.opt.prefix,
				Format:               s.opt.format,
				AutoExportKodoTokens: s.opt.tokens.KodoTokens,
			},
			ToTSDB: s.opt.enableTsdb,
			AutoExportToTSDBInput: pipeline.AutoExportToTSDBInput{
				OmitEmpty:            true,
				OmitInvalid:          false,
				IsMetric:             s.opt.isMetrics,
				ExpandAttr:           s.opt.expandAttr,
				RepoName:             s.opt.repoName,
				TSDBRepoName:         s.opt.tsdbReponame,
				SeriesName:           s.opt.tsdbSeriesName,
				SeriesTags:           s.opt.tsdbSeriesTags,
				Timestamp:            s.opt.tsdbTimestamp,
				AutoExportTSDBTokens: s.opt.tokens.TsDBTokens,
			},
			ForceDataConvert: s.opt.forceDataConvert,
		},
	}
	s.opt.tokenLock.RUnlock()
	schemas, se := s.client.PostDataSchemaFree(schemaFreeInput)
	if schemas != nil {
		s.updateSchemas(schemas)
	}

	if se != nil {
		nse, ok := se.(*reqerr.SendError)
		ste := &StatsError{
			ErrorDetail: se,
		}
		if ok {
			ste.LastError = nse.Error()
			ste.Errors = int64(len(nse.GetFailDatas()))
			ste.Success = int64(len(datas)) - ste.Errors
		} else {
			ste.LastError = se.Error()
			ste.Errors = int64(len(datas))
		}
		return ste
	}
	ste := &StatsError{
		ErrorDetail: se,
		StatsInfo: StatsInfo{
			Success:   int64(len(datas)),
			LastError: "",
		},
	}
	return ste
}