package com.microsoft.azure.cosmos.cassandrastress;

import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "loadtest", name = "sixmonth_max_daily_price_analysis")
public class SixmonthMaxDailyPriceAnalysis {
    public String product_id;
    public double max_of_max_daily_price;
    public double min_of_max_daily_price;
    public double avg_of_max_daily_price;
    public double avg_deviation_of_max_daily_price;
}
