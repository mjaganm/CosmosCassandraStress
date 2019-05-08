package com.microsoft.azure.cosmos.cassandrastress;

import com.datastax.driver.mapping.annotations.Table;

import java.util.Date;

@Table(keyspace = "loadtest", name = "daily_max_price_data")
public class DailyMaxPriceData {
    private String product_id;
    private String date;
    private double price;
    private long warehouse_id;
    private Date timestamp;

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getWarehouse_id() {
        return warehouse_id;
    }

    public void setWarehouse_id(long warehouse_id) {
        this.warehouse_id = warehouse_id;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
