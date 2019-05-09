package com.microsoft.azure.cosmos.cassandrastress;

import com.datastax.driver.mapping.annotations.Table;
import java.util.Date;

@Table(keyspace = "loadtest", name = "raw_price_data")
public class RawPriceData {
    private String product_id;
    private Date timestamp;
    private Boolean visible;
    private double price;
    private Long warehouse_id;
    private Integer seller_id;

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Boolean getVisible() {
        return visible;
    }

    public void setVisible(Boolean visible) {
        this.visible = visible;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Long getWarehouse_id() {
        return warehouse_id;
    }

    public void setWarehouse_id(Long warehouse_id) {
        this.warehouse_id = warehouse_id;
    }

    public Integer getSeller_id() {
        return seller_id;
    }

    public void setSeller_id(Integer seller_id) {
        this.seller_id = seller_id;
    }
}
