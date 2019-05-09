package com.microsoft.azure.cosmos.cassandrastress;

import com.datastax.driver.mapping.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadMode extends IOperationMode {
    private final Logger logger = LoggerFactory.getLogger(ReadMode.class);
    private final String operationName = "Reads";

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public String getOperationName() {
        return operationName;
    }

    @Override
    public void runDataOperation(String rowKey1, Mapper<RawPriceData> mapper_raw_price_data, Mapper<DailyMaxPriceData> mapper_daily_max_price_data) {
        mapper_raw_price_data.get(rowKey1);

        // mapper_raw_price_data.get
    }
}
