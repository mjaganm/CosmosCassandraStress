package com.microsoft.azure.cosmos.cassandrastress;

import org.apache.commons.lang3.RandomStringUtils;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

public class SchemaGenerator {

    private static Random rand = new Random();
    private static String GetRandomString(int length)
    {
        return RandomStringUtils.randomAlphanumeric(length);
    }

    public static RawPriceData getNewRawPriceData() {
        RawPriceData data = new RawPriceData();

        data.setProduct_id(GetRandomString(25));
        data.setTimestamp(new Timestamp(System.currentTimeMillis()));
        data.setVisible(rand.nextBoolean());
        data.setPrice(rand.nextDouble());
        data.setWarehouse_id(rand.nextLong());
        data.setSeller_id(rand.nextInt());

        return data;
    }

    public static DailyMaxPriceData getNewDailyMaxPriceData() {
        DailyMaxPriceData data = new DailyMaxPriceData();

        data.setProduct_id(GetRandomString(25));
        data.setDate(new Timestamp(System.currentTimeMillis()).toString());
        data.setPrice(rand.nextDouble());
        data.setWarehouse_id(rand.nextLong());
        data.setTimestamp(new Timestamp(System.currentTimeMillis()));

        return data;
    }
}
