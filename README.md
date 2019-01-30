# CosmosCassandraStress
CosmosCassandraStress is a tool to scale test the Cassandra API at very large throughput. It is to be observed that even at very large request volume, the latency stays significantly constant.

# Steps to run

1. Create Tables in the account

  CREATE TABLE loadtest.raw_price_data (
    product_id text,
    timestamp timestamp,
    visible boolean,
    price double,
    warehouse_id bigint,
    seller_id int,
    PRIMARY KEY (product_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC)
AND cosmosdb_provisioned_throughput = 1000000; // You could modify the throughput to be lower as well


CREATE TABLE loadtest.daily_max_price_data (
    product_id text,
    date text,
    price double,
    warehouse_id bigint,
    timestamp timestamp,
    PRIMARY KEY (product_id, date)
) WITH CLUSTERING ORDER BY (date ASC)
  AND cosmosdb_cell_level_timestamp = true
AND cosmosdb_provisioned_throughput = 1000000; // You could modify the throughput to be lower as well


com.microsoft.cosmos.cassandrastress.CassandraStress --endpoint <Cosmos Cassandra Account Name>.cassandra.cosmosdb.azure.com --key <Primary Key from Azure Portal> --username <Username from Azure Portal> --keyspace loadtest --tablename raw_price_data --operation get_ids -mode write_mode
