package com.microsoft.azure.cosmos.cassandrastress;

public class Constants {
    private static final String readMode = "read_mode";

    private static final String writeMode = "write_mode";

    private static final int numOfRetriesCassandra = 12;

    private static final int concurrentBatchSize = 1000;

    private static final long tokenChunkSize = (long) Math.pow(10, 15);

    public static String getReadMode() {
        return readMode;
    }

    public static String getWriteMode() {
        return writeMode;
    }

    public static int getNumOfRetriesCassandra() {
        return numOfRetriesCassandra;
    }

    public static int getConcurrentBatchSize() {
        return concurrentBatchSize;
    }

    public static long getTokenChunkSize() {
        return tokenChunkSize;
    }
}
