package com.microsoft.cosmos.cassandrastress;

import com.datastax.driver.mapping.annotations.Table;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Table(keyspace = "loadtest", name = "daily_max_price_data")
public class daily_max_price_data
{
    public String product_id;
    public String date;
    public double price;
    public long warehouse_id;
    public Date timestamp;
}
