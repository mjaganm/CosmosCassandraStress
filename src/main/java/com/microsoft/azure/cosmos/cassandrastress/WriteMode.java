package com.microsoft.azure.cosmos.cassandrastress;

import static com.datastax.driver.mapping.Mapper.Option.timestamp;

import com.datastax.driver.mapping.Mapper;
import java.util.Calendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteMode extends IOperationMode {
    private final Logger logger = LoggerFactory.getLogger(WriteMode.class);
    private final String operationName = "Writes";

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public String getOperationName() {
        return operationName;
    }

    @Override
    public void runDataOperation(String rowKey, Mapper<RawPriceData> mapperRawPriceData, Mapper<DailyMaxPriceData> mapperDailyMaxPriceData) {
        RawPriceData rawPriceData = SchemaGenerator.getNewRawPriceData();
        DailyMaxPriceData dailyMaxPriceData = SchemaGenerator.getNewDailyMaxPriceData();

        rawPriceData.setProduct_id(rowKey);
        dailyMaxPriceData.setProduct_id(rowKey);
        dailyMaxPriceData.setWarehouse_id(rawPriceData.getWarehouse_id());

        // Truncate the Date to day
        Calendar cal = Calendar.getInstance();
        cal.setTime(dailyMaxPriceData.getTimestamp());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        dailyMaxPriceData.setTimestamp(cal.getTime());

        mapperRawPriceData.save(rawPriceData);
        mapperDailyMaxPriceData.save(
                dailyMaxPriceData,
                timestamp(Double.doubleToLongBits(rawPriceData.getPrice() * 10000)));
    }
}
