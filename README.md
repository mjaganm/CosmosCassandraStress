# CosmosCassandraStress
CosmosCassandraStress is a tool to scale test the Cassandra API at very large throughput. It is to be observed that even at very large request volume, the latency stays significantly constant.

# Steps to run
com.microsoft.cosmos.cassandrastress.CassandraStress --endpoint <Cosmos Cassandra Account Name>.cassandra.cosmosdb.azure.com --key <Primary Key from Azure Portal> --username <Username from Azure Portal> --keyspace loadtest --tablename raw_price_data --operation get_ids -mode write_mode
