package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/ghodss/yaml"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
)

var yamlConfig Config

const collector = "cloudwatch_log_export"

var lastEndTimeFile = "last_end_time.txt"

type Config struct {
	Group     string
	Query     string
	Metrics map[string]struct {
		Type        string
		Description string
		Labels      []string
		Value       string
		metricDesc  *prometheus.Desc
	}
}

type QueryCollector struct {}

func (e *QueryCollector) Describe(ch chan<- *prometheus.Desc) {
	for metricName, metric := range yamlConfig.Metrics {
		metric.metricDesc = prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", metricName),
			metric.Description,
			metric.Labels, nil,
		)
		yamlConfig.Metrics[metricName] = metric
		log.Printf("metric description for \"%s\" registered", metricName)
	}
}

func (e *QueryCollector) Collect(ch chan<- prometheus.Metric) {

	cfg, awsError := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("ap-south-1"), // Replace with your AWS region
		config.WithSharedConfigProfile("amazon"),
	)
	if awsError != nil {
		log.Fatalf("Error loading AWS configuration: %v", awsError)
	}

	client := cloudwatchlogs.NewFromConfig(cfg)


	logMetric := fetchAndProcessLogs(context.TODO(), client)

	log.Printf("%d",len(logMetric))

	for index, lMetric := range logMetric {
		for name, metric := range yamlConfig.Metrics {
			// Metric labels
			labelValues := []string{}
			
			for _, label := range metric.Labels {
				labelValues = append(labelValues, lMetric[label])
			}

			// Add metric
			switch strings.ToLower(metric.Type) {
			case "counter":
				ch <- prometheus.MustNewConstMetric(metric.metricDesc, prometheus.CounterValue, float64(index), labelValues...)
			case "gauge":
				ch <- prometheus.MustNewConstMetric(metric.metricDesc, prometheus.GaugeValue, float64(index), labelValues...)
			default:
				log.Panicf("Fail to add metric for %s: %s is not valid type", name, metric.Type)
				continue
			}
		}
	}
}


func readLastEndTimeFromFile() int64 {
	content, err := os.ReadFile(lastEndTimeFile)
	if err != nil {
		log.Printf("Error reading lastEndTime file: %v", err)
		return 0
	}

	lastEndTime, err := strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		log.Printf("Error parsing lastEndTime from file: %v", err)
		return 0
	}

	return lastEndTime
}

func writeLastEndTimeToFile(lastEndTime int64) {
	err := os.WriteFile(lastEndTimeFile, []byte(strconv.FormatInt(lastEndTime, 10)), 0644)
	if err != nil {
		log.Printf("Error writing lastEndTime to file: %v", err)
	}
}


func fetchAndProcessLogs(ctx context.Context, client *cloudwatchlogs.Client) ([]map[string]string) {
	currentTime := time.Now()
	startTime := aws.Int64((currentTime.UnixMilli() / 1000) - 86400)
	endTime := aws.Int64(currentTime.UnixMilli() / 1000)

	lastEndTime := readLastEndTimeFromFile()

	// If lastEndTime is set, use it as the new StartTime
	if lastEndTime != 0 {
		startTime = aws.Int64(lastEndTime)
	}
	startQueryInput := &cloudwatchlogs.StartQueryInput{
		LogGroupName: aws.String(yamlConfig.Group),
		StartTime:    startTime, // 12 hours ago
		EndTime:      endTime,
		QueryString:  aws.String(yamlConfig.Query),
	}

	startQueryOutput, err := client.StartQuery(ctx, startQueryInput)
	resultSlice := make([]map[string]string, 0)
	if err != nil {
		log.Printf("Error starting CloudWatch Logs query: %v", err)
		return resultSlice
	}

	queryID := startQueryOutput.QueryId

		getQueryResultsInput := &cloudwatchlogs.GetQueryResultsInput{
			QueryId: queryID,
		}

		for {
			resultsResp, err := client.GetQueryResults(ctx, getQueryResultsInput)
			if err != nil {
				log.Printf("Error getting CloudWatch Logs query results: %v", err)
				return resultSlice;
			}

			log.Println(resultsResp.Status);


			if resultsResp.Status == types.QueryStatusComplete {
				// The query has completed, exit the loop
				for _, result := range resultsResp.Results {
					// Convert result to a map[string]string
					resultMap := convertResultToMap(result)
					resultSlice = append(resultSlice, resultMap)
				}
				writeLastEndTimeToFile(currentTime.UnixMilli() / 1000)
				break
			}
			time.Sleep(2 * time.Second)
		}

	return resultSlice;
}

// Helper function to convert ResultField to a map[string]string
func convertResultToMap(result []types.ResultField) map[string]string {
    resultMap := make(map[string]string)
    for _, field := range result {
        if field.Value != nil && field.Field != nil {
            resultMap[*field.Field] = *field.Value
        }
    }
    return resultMap
}


func main() {
	
	var err error
	var configFile, bind string
	
	flag.StringVar(&configFile, "config", "config.yml", "configuration file")
	flag.StringVar(&bind, "bind", "0.0.0.0:9104", "bind")
	flag.Parse()

	// =====================
	// Load config & yaml
	// =====================
	var b []byte
	if b, err = os.ReadFile(configFile); err != nil {
		log.Fatalf("Failed to read config file: %s", err)
		os.Exit(1)
	}

	// Load yaml
	if err := yaml.Unmarshal(b, &yamlConfig); err != nil {
		log.Fatalf("Failed to load config: %s", err)
		os.Exit(1)
	}

	log.Printf("Register version collector - %s", collector)
	prometheus.Register(version.NewCollector(collector))
	prometheus.Register(&QueryCollector{})

	// Register http handler
	log.Printf("HTTP handler path - %s", "/metrics")
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		h := promhttp.HandlerFor(prometheus.Gatherers{
			prometheus.DefaultGatherer,
		}, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	})

	// start server
	log.Printf("Starting http server - %s", bind)
	if err := http.ListenAndServe(bind, nil); err != nil {
		log.Panicf("Failed to start http server: %s", err)
	}

}