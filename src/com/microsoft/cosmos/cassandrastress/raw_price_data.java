package com.microsoft.cosmos.cassandrastress;

import com.datastax.driver.mapping.annotations.Table;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

@Table(keyspace = "loadtest", name = "raw_price_data")
public class raw_price_data
{
    public String product_id;
    public Date timestamp;
    public Boolean visible;
    public double price;
    public Long warehouse_id;
    public Integer seller_id;
}
