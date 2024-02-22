package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	jobName      = "cloudwatchlogs"
	instance     = "amazon"
	cwLogGroup   = "apiconnect-staging_asc_api"
	pushGateway  = "http://localhost:9091"
	customMetric = "api_success_rate"
)

var yourCustomMetric *prometheus.GaugeVec

func init() {
	yourCustomMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: customMetric,
			Help: "Your custom metric description",
		},
		[]string{"error_code", "url", "shop_id", "home_shop_id"},
	)
}

func fetchAndProcessLogs(ctx context.Context, client *cloudwatchlogs.Client) {
	startQueryInput := &cloudwatchlogs.StartQueryInput{
		LogGroupName: &cwLogGroup,
		StartTime:    aws.Int64((time.Now().UnixNano() / int64(time.Millisecond)) - (5 * 60 * 60 * 1000)), // 5 minutes ago
		EndTime:      aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
		// QueryString:  aws.String(`fields @timestamp, message | filter message like /error_code/ and message like /_url/ | parse message "=> error_code * => {\"_url\":\"\/*\",\"shop_id\":\"*\",\"home_shop_id\":\"*\"" as error_code, url, shop_id, home_shop_id`),
		QueryString: aws.String(`fields @timestamp, message`),
	}

	startQueryOutput, err := client.StartQuery(ctx, startQueryInput)
	if err != nil {
		log.Printf("Error starting CloudWatch Logs query: %v", err)
		return
	}

	queryID := startQueryOutput.QueryId

	for {
		getQueryResultsInput := &cloudwatchlogs.GetQueryResultsInput{
			QueryId: queryID,
		}

		resultsResp, err := client.GetQueryResults(ctx, getQueryResultsInput)
		if err != nil {
			log.Printf("Error getting CloudWatch Logs query results: %v", err)
			return
		}

		for _, result := range resultsResp.Results {
			// Convert result to a map[string]string
			resultMap := convertResultToMap(result)

			// Extract values based on your log format
			message := getStringValue(resultMap, "message");
			log.Print(message);
			errorCode := getStringValue(resultMap, "error_code")
			url := getStringValue(resultMap, "url")
			shopID := getStringValue(resultMap, "shop_id")
			homeShopID := getStringValue(resultMap, "home_shop_id")
	
			// Process your values (e.g., push to Prometheus)
			processValues(errorCode, url, shopID, homeShopID)
		}

		if resultsResp.Status == types.QueryStatusComplete {
			break
		}

		time.Sleep(2 * time.Second) // Sleep before checking results again
	}
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

func getStringValue(result map[string]string, key string) string {
	if value, ok := result[key]; ok {
		return value
	}
	return ""
}

func processValues(errorCode, url, shopID, homeShopID string) {
	// Process your values (e.g., push to Prometheus)
	// In this example, pushing to Prometheus is shown
	yourCustomMetric.WithLabelValues(errorCode, url, shopID, homeShopID).Set(1)
}

func pushMetrics() {
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(yourCustomMetric)

	gatherers := prometheus.Gatherers{reg}

	pusher := push.New(pushGateway, jobName).
		Gatherer(gatherers).
		Grouping("instance", instance)

	err := pusher.Add()
	if err != nil {
		log.Printf("Error pushing metrics: %v", err)
	}
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
        config.WithRegion("ap-south-1"), // Replace with your AWS region
        config.WithSharedConfigProfile("amazon"),
    )
	log.Printf(cfg.AppID);
	if err != nil {
		log.Fatalf("Error loading AWS configuration: %v", err)
	}

	client := cloudwatchlogs.NewFromConfig(cfg)

	// Fetch and process logs every 1 minute
	for range time.Tick(1 * time.Minute) {
		fetchAndProcessLogs(context.TODO(), client)
		pushMetrics()
	}
}
