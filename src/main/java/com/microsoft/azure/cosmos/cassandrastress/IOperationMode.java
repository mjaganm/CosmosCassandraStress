package com.microsoft.azure.cosmos.cassandrastress;

import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.mapping.Mapper;
import com.microsoft.applicationinsights.TelemetryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IOperationMode {
    private final Logger logger = LoggerFactory.getLogger(IOperationMode.class);
    private final String operationName = "IOperation";

    public Logger getLogger() {
        return logger;
    }

    public String getOperationName() {
        return operationName;
    }

    public void runDataOperation(String rowKey1, Mapper<RawPriceData> mapper_raw_price_data, Mapper<DailyMaxPriceData> mapper_daily_max_price_data) throws OverloadedException, IllegalStateException, Exception {
    }

    void publishMetricP99(TelemetryClient telemetryClient, double p99Value) {
        getLogger().info(getOperationName() +  " p99 Latency: " + p99Value);

        if (telemetryClient != null) {
            telemetryClient.trackMetric(getOperationName() + "P99Latency", p99Value);
        }
    }

    void publishMetricP90(TelemetryClient telemetryClient, double p90Value) {
        getLogger().info(getOperationName() +  " p90 Latency: " +  p90Value);

        if (telemetryClient != null) {
            telemetryClient.trackMetric(getOperationName() + "P90Latency", p90Value);
        }
    }

    void publishMetricP50(TelemetryClient telemetryClient, double p50Value) {
        getLogger().info(getOperationName() +  " p50 Latency: " + p50Value);

        if (telemetryClient != null) {
            telemetryClient.trackMetric(getOperationName() + "P50Latency", p50Value);
        }
    }

    void publishAverageLatency(TelemetryClient telemetryClient, double avgLatency) {
        getLogger().info(getOperationName() +  " Avg Latency: "  + avgLatency);

        if (telemetryClient != null) {
            telemetryClient.trackMetric(getOperationName() + "AvgLatency", avgLatency);
        }
    }
}
