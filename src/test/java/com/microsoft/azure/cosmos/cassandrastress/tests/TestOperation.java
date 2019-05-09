package com.microsoft.azure.cosmos.cassandrastress.tests;

import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.mapping.Mapper;
import com.microsoft.azure.cosmos.cassandrastress.DailyMaxPriceData;
import com.microsoft.azure.cosmos.cassandrastress.IOperationMode;
import com.microsoft.azure.cosmos.cassandrastress.RawPriceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class TestOperation extends IOperationMode {
    private final Logger logger = LoggerFactory.getLogger(com.microsoft.azure.cosmos.cassandrastress.tests.TestOperation.class);
    private final String operationName = "Test";

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public String getOperationName() {
        return operationName;
    }

    @Override
    public void runDataOperation(String rowKey, Mapper<RawPriceData> mapperRawPriceData, Mapper<DailyMaxPriceData> mapperDailyMaxPriceData) throws Exception{
        logger.info("Test operation: " + rowKey);

        if (rowKey.equals("throwOverloadedException")) {
            throw new OverloadedException(new InetSocketAddress("localhost", 10350), "throttle");
        } else if (rowKey.equals("throwException")) {
            throw new Exception();
        }
    }
}
