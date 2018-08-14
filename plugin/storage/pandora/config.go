package pandora

import (
	//"errors"

	pipelinebase "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/pipeline"
)
type Configuration struct {
	Servers  string
	ak       string
	sk       string
	repoName string
}


// NewClient creates a new ElasticSearch client
func (c *Configuration) NewClient() (pipeline.PipelineAPI, error) {
	// 生成配置文件
	cfg := pipeline.NewConfig().
		WithAccessKeySecretKey(c.ak, c.sk).
		WithEndpoint(c.Servers).
		WithLogger(pipelinebase.NewDefaultLogger()).
		WithLoggerLevel(pipelinebase.LogDebug)

	// 生成client实例
	client, err := pipeline.New(cfg)
	if err != nil {
		//log.Println(err)
		return nil,err
	}
	return client,nil

	//if len(c.Servers) < 1 {
	//	return nil, errors.New("No servers specified")
	//}
	//rawClient, err := elastic.NewClient(c.GetConfigs()...)
	//if err != nil {
	//	return nil, err
	//}
	//
	//sm := storageMetrics.NewWriteMetrics(metricsFactory, "bulk_index")
	//m := sync.Map{}
	//
	//service, err := rawClient.BulkProcessor().
	//	Before(func(id int64, requests []elastic.BulkableRequest) {
	//	m.Store(id, time.Now())
	//}).
	//	After(func(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	//	start, ok := m.Load(id)
	//	if !ok {
	//		return
	//	}
	//	m.Delete(id)
	//
	//	duration := time.Since(start.(time.Time))
	//	sm.Emit(err, duration)
	//
	//	if err != nil {
	//		failed := len(response.Failed())
	//		total := len(requests)
	//		logger.Error("Elasticsearch could not process bulk request",
	//			zap.Int("request_count", total),
	//			zap.Int("failed_count", failed),
	//			zap.Error(err),
	//			zap.Any("response", response))
	//	}
	//}).
	//	BulkSize(c.BulkSize).
	//	Workers(c.BulkWorkers).
	//	BulkActions(c.BulkActions).
	//	FlushInterval(c.BulkFlushInterval).
	//	Do(context.Background())
	//if err != nil {
	//	return nil, err
	//}
	//return es.WrapESClient(rawClient, service), nil
}

func newPandoraSender(opt *PandoraOption) (s *PandoraSender, err error) {
	logger := pipelinebase.NewDefaultLogger()
	config := pipeline.NewConfig().
		WithPipelineEndpoint(opt.endpoint).
		WithAccessKeySecretKey(opt.ak, opt.sk).
		WithLogger(logger).
		WithLoggerLevel(pipelinebase.LogInfo).
		WithRequestRateLimit(opt.reqRateLimit).
		WithFlowRateLimit(opt.flowRateLimit).
		WithGzipData(opt.gzip).
		WithHeaderUserAgent(opt.useragent).WithInsecureServer(opt.insecureServer)
	if opt.logdbendpoint != "" {
		config = config.WithLogDBEndpoint(opt.logdbendpoint)
	}
	client, err := pipeline.New(config)
	if err != nil {
		err = fmt.Errorf("cannot init pipelineClient %v", err)
		return
	}
	if opt.reqRateLimit > 0 {
		log.Warnf("Runner[%v] Sender[%v]: you have limited send speed within %v requests/s", opt.runnerName, opt.name, opt.reqRateLimit)
	}
	if opt.flowRateLimit > 0 {
		log.Warnf("Runner[%v] Sender[%v]: you have limited send speed within %v KB/s", opt.runnerName, opt.name, opt.flowRateLimit)
	}
	userSchema := parseUserSchema(opt.repoName, opt.schema)
	s = &PandoraSender{
		opt:        *opt,
		client:     client,
		alias2key:  make(map[string]string),
		UserSchema: userSchema,
		schemas:    make(map[string]pipeline.RepoSchemaEntry),
		extraInfo:  utilsos.GetExtraInfo(),
	}

	expandAttr := make([]string, 0)
	if s.opt.isMetrics {
		s.opt.tsdbTimestamp = metric.Timestamp
		metricTags := metric.GetMetricTags()
		if s.opt.extraInfo {
			// 将 osInfo 添加到每个 metric 的 tags 中
			for key, val := range metricTags {
				val = append(val, osInfo...)
				metricTags[key] = val
			}

			// 将 osInfo 中的字段导出到每个 series 中
			expandAttr = append(expandAttr, osInfo...)
		}
		s.opt.tsdbSeriesTags = metricTags
		s.opt.analyzerInfo.Default = logdb.KeyWordAnalyzer
	} else if len(s.opt.analyzerInfo.Analyzer) > 4 {
		//超过4个的情况代表有用户手动输入了字段的分词方式，此时不开启全文索引
		s.opt.analyzerInfo.FullText = false
	} else {
		s.opt.analyzerInfo.FullText = true
	}
	s.opt.expandAttr = expandAttr

	dsl := strings.TrimSpace(opt.autoCreate)
	schemas, err := pipeline.DSLtoSchema(dsl)
	if err != nil {
		log.Errorf("Runner[%v] Sender[%v]: auto create pandora repo error: %v, you can create on pandora portal, ignored...", opt.runnerName, opt.name, err)
		err = nil
	}
	if initErr := s.client.InitOrUpdateWorkflow(&pipeline.InitOrUpdateWorkflowInput{
		// 此处要的 schema 为 autoCreate 中用户指定的，所以 SchemaFree 要恒为 true
		InitOptionChange: true,
		SchemaFree:       true,
		Region:           s.opt.region,
		WorkflowName:     s.opt.workflowName,
		RepoName:         s.opt.repoName,
		Schema:           schemas,
		SchemaFreeToken:  s.opt.tokens.SchemaFreeTokens,
		RepoOptions:      &pipeline.RepoOptions{WithIP: s.opt.withip, UnescapeLine: s.opt.UnescapeLine},
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
	}); initErr != nil {
		log.Errorf("runner[%v] Sender [%v]: init Workflow error %v", opt.runnerName, opt.name, initErr)
	}
	s.UpdateSchemas()
	return
}


// PandoraOption 创建Pandora Sender的选项
type PandoraOption struct {
	runnerName   string
	name         string
	repoName     string
	workflowName string
	region       string
	endpoint     string
	ak           string
	sk           string
	schema       string
	schemaFree   bool   // schemaFree在用户数据有新字段时就更新repo添加字段，如果repo不存在，创建repo。schemaFree功能包含autoCreate
	autoCreate   string // 自动创建用户的repo，dsl语言填写schema
	//updateInterval time.Duration
	reqRateLimit  int64
	flowRateLimit int64
	gzip          bool
	uuid          bool
	withip        string
	extraInfo     bool

	enableLogdb   bool
	logdbReponame string
	logdbendpoint string
	//analyzerInfo  pipeline.AnalyzerInfo

	enableTsdb     bool
	tsdbReponame   string
	tsdbSeriesName string
	tsdbendpoint   string
	tsdbTimestamp  string
	tsdbSeriesTags map[string][]string

	enableKodo bool
	bucketName string
	email      string
	prefix     string
	format     string

	forceMicrosecond   bool
	forceDataConvert   bool
	ignoreInvalidField bool
	autoConvertDate    bool
	useragent          string
	logkitSendTime     bool
	UnescapeLine       bool
	insecureServer     bool

	isMetrics      bool
	numberUseFloat bool
	expandAttr     []string

	//tokens    Tokens
	//tokenLock *sync.RWMutex
}
