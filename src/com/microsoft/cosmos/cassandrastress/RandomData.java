package com.microsoft.cosmos.cassandrastress;

import org.apache.commons.lang3.RandomStringUtils;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.*;

public class RandomData
{
    private static String AlphaNum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static String AlphaNumSpace = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";

    private static Random rand = new Random();

    public static String GetRandomString(int length)
    {
        return RandomStringUtils.randomAlphanumeric(length);
    }

    private static int GetRandomInt()
    {
        return rand.nextInt(50);
    }

    private static int GetRandomInt(Integer length)
    {
        return rand.nextInt(length);
    }

    private static ArrayList<String> GetRandomStringList()
    {
        ArrayList<String> data = new ArrayList<String>();

        data.add(GetRandomString(15));
        data.add(GetRandomString(15));
        data.add(GetRandomString(15));
        data.add(GetRandomString(15));
        data.add(GetRandomString(15));
        data.add(GetRandomString(15));

        return data;
    }

    public static raw_price_data GetNew_raw_price_data()
    {
        raw_price_data data = new raw_price_data();

        data.product_id = GetRandomString(25);

        data.timestamp = new Timestamp(System.currentTimeMillis());

        data.visible = rand.nextBoolean();

        data.price = rand.nextDouble();

        data.warehouse_id = rand.nextLong();

        data.seller_id = rand.nextInt();

        return data;
    }

    public static daily_max_price_data GetNew_daily_max_price_data()
    {
        daily_max_price_data data = new daily_max_price_data();

        data.product_id = GetRandomString(25);

        data.date = new String(new Timestamp(System.currentTimeMillis()).toString());

        data.price = rand.nextDouble();

        data.warehouse_id = rand.nextLong();

        data.timestamp = new Timestamp(System.currentTimeMillis());

        return data;
    }
}
