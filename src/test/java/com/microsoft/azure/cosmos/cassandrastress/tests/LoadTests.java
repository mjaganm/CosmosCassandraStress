package com.microsoft.azure.cosmos.cassandrastress.tests;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.microsoft.azure.cosmos.cassandrastress.CassandraStress;
import com.microsoft.azure.cosmos.cassandrastress.DailyMaxPriceData;
import com.microsoft.azure.cosmos.cassandrastress.RawPriceData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LoadTests {

    private CassandraStress currentStressRun;

    public CassandraStress getCurrentStressRun() {
        return currentStressRun;
    }

    public void setCurrentStressRun(CassandraStress currentStressRun) {
        this.currentStressRun = currentStressRun;
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {

        String sampleArgs = "com.microsoft.azure.cosmos.cassandrastress.CassandraStress --endpoint jaganma-cassandra5.cassandra.cosmosdb.azure.com --key qpNtS4FHUurT0KiC806eMZBqX6gmxR1NXaSJndozH0zjL6QorXgtWuOFI2KqN4828IKGzrYs7ql2cR88WWGcXA== --username jaganma-cassandra5 --keyspace loadtest --tablename raw_price_data --appInsightsKey db41ac6a-45ea-4d42-95bd-bb1cd2075d32 -mode write_mode";

        setCurrentStressRun(new CassandraStress(sampleArgs.split(" ")));
        if (!getCurrentStressRun().initialize()) {
            System.out.println("Fatal Initialization error. Exiting...");
            return;
        }
    }

    @AfterMethod
    public void afterMethod() {

    }
    
    @Test
    public void isInitialized() {
        assertThat(getCurrentStressRun().isAppInsightsEnabled()).isEqualTo(true);
    }

    @Test
    public void testRunDataOperation() throws Exception {
        TestOperation testOperation = new TestOperation();

        MappingManager manager = new MappingManager(getCurrentStressRun().getCurrentSession());
        Mapper<RawPriceData> mapperRawPriceData = manager.mapper(RawPriceData.class);
        Mapper<DailyMaxPriceData> mapperDailyMaxPriceData = manager.mapper(DailyMaxPriceData.class);

        assertThat(getCurrentStressRun().runOperationWithRetries(testOperation, "test", mapperRawPriceData, mapperDailyMaxPriceData)).isTrue();
    }

    @Test
    public void testRunDataOperationOverloadedException() throws Exception {
        TestOperation testOperation = new TestOperation();

        MappingManager manager = new MappingManager(getCurrentStressRun().getCurrentSession());
        Mapper<RawPriceData> mapperRawPriceData = manager.mapper(RawPriceData.class);
        Mapper<DailyMaxPriceData> mapperDailyMaxPriceData = manager.mapper(DailyMaxPriceData.class);

        getCurrentStressRun().runOperationWithRetries(testOperation, "throwOverloadedException", mapperRawPriceData, mapperDailyMaxPriceData);

        getCurrentStressRun().runOperationWithRetries(testOperation, "throwException", mapperRawPriceData, mapperDailyMaxPriceData);
    }
}
