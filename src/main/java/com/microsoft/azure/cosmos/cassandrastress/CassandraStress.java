package com.microsoft.azure.cosmos.cassandrastress;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.StandardSystemProperty;
import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.applicationinsights.TelemetryConfiguration;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStress {
    private static Random random = new Random();

    private final Logger logger = LoggerFactory.getLogger(CassandraStress.class);
    private final JCommander jcommander;
    @Parameter
    private List<String> parameters = new ArrayList<>();

    @Parameter(
            names = {"--endpoint", "-e"},
            description = "Cosmos DB Endpoint"
    )
    private String accountUrl = "";

    @Parameter(
            names = {"--key", "-k"},
            description = "Cosmos DB Key"
    )
    private String accountKey = "";

    @Parameter(
            names = {"--username", "-user"},
            description = "Cassandra UserName"
    )
    private String userName = "cassandra";

    @Parameter(
            names = {"--keyspace", "-ks"},
            description = "Cosmos DB Database"
    )
    private String keyspace = "";// "cluster";

    @Parameter(
            names = {"--tablename", "-table"},
            description = "Cosmos DB Collection"
    )
    private String tableName = "";

    @Parameter(
            names = {"--help"},
            help = true,
            description = "Usage"
    )
    private boolean help = false;

    @Parameter(
            names = {"--mode", "-mode"},
            description = "Read/Write mode"
    )
    private String mode = "read_mode";

    @Parameter(
            names = {"--appInsightsKey", "-InstrumentationKey"},
            description = "Instrumentation Key for AppInsights"
    )
    private String instrumentationKey = "";

    private Map<Integer, Long> currentTokenRanges = new HashMap<>();
    private String primaryKey = "product_id";
    private boolean isAppInsightsEnabled;
    private IOperationMode currentOperation;
    private TelemetryClient telemetryClient;
    private Cluster cluster;
    private Session currentSession;

    public CassandraStress(String[] args) {
        jcommander = new JCommander(this, args);
    }

    public boolean isAppInsightsEnabled() {
        return isAppInsightsEnabled;
    }

    public void setAppInsightsEnabled(boolean appInsightsEnabled) {
        isAppInsightsEnabled = appInsightsEnabled;
    }

    public IOperationMode getCurrentOperation() {
        return currentOperation;
    }

    public void setCurrentOperation(IOperationMode currentOperation) {
        this.currentOperation = currentOperation;
    }

    public TelemetryClient getTelemetryClient() {
        return telemetryClient;
    }

    public void setTelemetryClient(TelemetryClient telemetryClient) {
        this.telemetryClient = telemetryClient;
    }

    public String getInstrumentationKey() {
        return instrumentationKey;
    }

    public static Random getRandom() {
        return random;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Session getCurrentSession() {
        return currentSession;
    }

    public void setCurrentSession(Session currentSession) {
        this.currentSession = currentSession;
    }

    public void runExponentialBackoff(int numOfAttempts) {
        // Ceiling for Exponential backoff
        if (numOfAttempts > Constants.getNumOfRetriesCassandra()) {
            numOfAttempts = Constants.getNumOfRetriesCassandra();
        }

        int waitTimeInMilliSeconds = (int) Math.pow(2, numOfAttempts) * 100;

        logger.info("Exponential backoff: Retrying after Waiting for " + waitTimeInMilliSeconds + " milliseconds");

        try {
            Thread.sleep(waitTimeInMilliSeconds);
        }
        catch (InterruptedException iex) {
            logger.debug("InterruptedException trying to do Exponential backoff");
        }
    }

    public boolean initialize() throws Exception {
        if (help) {
            jcommander.usage();
            return false;
        }

        if (!Constants.getWriteMode().equals(mode) && !Constants.getReadMode().equals(mode)) {
            System.out.println("Mode is neither Read nor Write: " + mode);
            jcommander.usage();
            return false;
        }

        IOperationMode currentOperation = null;
        if (Constants.getWriteMode().equals(mode)) {
            currentOperation = new WriteMode();
        } else if (Constants.getReadMode().equals(mode)) {
            currentOperation = new ReadMode();
        }

        setCurrentOperation(currentOperation);

        if (!instrumentationKey.isEmpty()) {
            setAppInsightsEnabled(true);
        }

        TelemetryClient telemetryClient = null;
        if (isAppInsightsEnabled()) {
            telemetryClient = new TelemetryClient();
            telemetryClient.getContext().getUser().setId("cosmos");
            telemetryClient.getContext().getDevice().setOperatingSystem(StandardSystemProperty.OS_NAME.value());
            telemetryClient.getContext().getDevice().setOperatingSystemVersion(StandardSystemProperty.OS_VERSION.value());
            TelemetryConfiguration.getActive().setInstrumentationKey(getInstrumentationKey());
        }

        setTelemetryClient(telemetryClient);

        generateTokenRanges();

        setCurrentSession(connectToCassandra());

        return true;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("CosmosCassandraStress run...");

        CassandraStress stressRun = new CassandraStress(args);
        if (!stressRun.initialize()) {
            System.out.println("Fatal Initialization error. Exiting...");
            return;
        }

        Session currentSession = stressRun.connectToCassandra();
        Map<String, String> keysToQuery = stressRun.getKeysForLoadTest();

        // Add jitter before starting loadrun on each instance
        Thread.sleep(getRandom().nextInt(10000));

        // Start the load run
        stressRun.runLoadTest(keysToQuery, currentSession);
    }

    private void runLoadTest(Map<String, String> keysToUse, Session currentSession) throws InterruptedException, Exception {
        // Add jitter before starting loadrun
        Thread.sleep(getRandom().nextInt(1000));

        ExecutorService service = Executors.newScheduledThreadPool(100);

        List<String> keysToQueryShuffled = new ArrayList<>(keysToUse.values());
        Collections.shuffle(keysToQueryShuffled, getRandom());

        // Remove the second half of list for randomizing calls for loadtest purposes
        int listSize = keysToQueryShuffled.size();
        keysToQueryShuffled = keysToQueryShuffled.subList(listSize / 4, listSize * 3 / 4);

        Map<Integer, String> localSetForKeys = new ConcurrentHashMap<>();
        int index = 0;

        MappingManager manager = new MappingManager(currentSession);
        Mapper<RawPriceData> mapperRawPriceData = manager.mapper(RawPriceData.class);
        Mapper<DailyMaxPriceData> mapperDailyMaxPriceData = manager.mapper(DailyMaxPriceData.class);

        for (String rowKey : keysToQueryShuffled) {
            localSetForKeys.put(index, rowKey);
            index++;

            if (index == Constants.getConcurrentBatchSize()) {
                Map<UUID, Double> batchLatenciesMap = new ConcurrentHashMap<>();
                CountDownLatch latch = new CountDownLatch(Constants.getConcurrentBatchSize());

                LocalDateTime startTimeBatch = LocalDateTime.now();
                logger.debug("Started Batch execution : " + startTimeBatch);

                for (String rowKey1 : localSetForKeys.values()) {
                    service.submit(() -> {
                        StopWatch stopwatch = StopWatch.createStarted();

                        // execution single operation with retries
                        runOperationWithRetries(getCurrentOperation(), rowKey1, mapperRawPriceData, mapperDailyMaxPriceData);

                        long timeConsumed = stopwatch.getTime(TimeUnit.MILLISECONDS);
                        stopwatch.stop();

                        batchLatenciesMap.put(UUID.randomUUID(), (double) timeConsumed);

                        latch.countDown();
                    });
                }

                // Wait for the local set of reads to complete
                if (latch != null) {
                    if (!latch.await(10, TimeUnit.SECONDS)) {
                        logger.error("Wait time elapsed before batch requests completed");
                    }
                }

                LocalDateTime completedTimeBatch = LocalDateTime.now();
                logger.debug("Completed Batch execution : " + completedTimeBatch);

                long timeConsumedInMilliSeconds = ChronoUnit.MILLIS.between(startTimeBatch, completedTimeBatch);
                logger.info("Batch Execution of size: " + Constants.getConcurrentBatchSize() + " took milli-seconds :" + timeConsumedInMilliSeconds);

                // Parse and publish Latencies of calls
                publishLatencyNumbers(getCurrentOperation(), batchLatenciesMap);

                // Cleanup
                batchLatenciesMap.clear();
                localSetForKeys.clear();
                index = 0;
            }
        }
    }

    public boolean runOperationWithRetries(IOperationMode currentOperation, String rowKey1, Mapper<RawPriceData> mapper_raw_price_data, Mapper<DailyMaxPriceData> mapper_daily_max_price_data) {
        Boolean needRetry = true;
        int numOfAttempts = 0;
        while (numOfAttempts < Constants.getNumOfRetriesCassandra() && needRetry) {
            try {
                currentOperation.runDataOperation(rowKey1, mapper_raw_price_data, mapper_daily_max_price_data);
                needRetry = false;
            } catch (OverloadedException oex) {
                logger.info("OverloadedException: {} Retrying...", oex);
            } catch (IllegalStateException ie) {
                Boolean isThrottled = HandleIfThrottle(ie);

                if (!isThrottled) {
                    logger.info("Had hit IllegalStateException exception: {}", ie);
                    throw ie;
                }
            } catch (Exception unhandledException) {
                logger.info("Had hit unhandled exception: {}", unhandledException);
                // throw unhandledException;
            }

            if (needRetry) {
                runExponentialBackoff(numOfAttempts);
                numOfAttempts++;
            }
        }

        if (numOfAttempts == Constants.getNumOfRetriesCassandra() && needRetry) {
            return false;
        }

        return true;
    }

    private void publishLatencyNumbers(IOperationMode currentOperation, Map<UUID, Double> batchLatenciesMap) {
        // Parse the latencies
        logger.info("Number of requests succeeded: " + batchLatenciesMap.size());
        Double sumOfLatencies = 0.0;
        for (Double val : batchLatenciesMap.values()) {
            sumOfLatencies += val;
        }

        Double avgLatency = sumOfLatencies / batchLatenciesMap.size();
        currentOperation.publishAverageLatency(getTelemetryClient(), avgLatency);

        // Calculate p50/p70/p90/p99
        List<Double> dataSet = new ArrayList<Double>(batchLatenciesMap.values());
        int datasetSize = dataSet.size();

        Collections.sort(dataSet, Collections.reverseOrder());

        Double p50 = dataSet.get(datasetSize / 2);
        currentOperation.publishMetricP50(getTelemetryClient(), p50);

        Double p90 = dataSet.get(datasetSize / 10);
        currentOperation.publishMetricP90(getTelemetryClient(), p90);

        Double p99 = dataSet.get(datasetSize / 100);
        currentOperation.publishMetricP99(getTelemetryClient(), p99);
    }

    private Boolean HandleIfThrottle(IllegalStateException e) {
        Throwable exception = e;
        Boolean isThrottled = false;
        while (!(exception instanceof DocumentClientException) && exception != null) {
            exception = e.getCause();
        }

        if (exception != null) {
            DocumentClientException documentClientException = (DocumentClientException) exception;
            if (documentClientException.getStatusCode() == HttpConstants.StatusCodes.TOO_MANY_REQUESTS) {
                logger.info("IllegalStateException: Throttled. Retrying...");
                return true;
            }
        }

        return isThrottled;
    }

    private Session connectToCassandra() throws Exception {
        return connect(accountUrl, userName, accountKey);
    }

    private Map<String, String> generateRowKeys() throws InterruptedException {
        Map<String, String> randomKeysGenerated = new ConcurrentHashMap<>();

        ExecutorService service = Executors.newScheduledThreadPool(100);
        CountDownLatch latch = new CountDownLatch(1000);

        logger.info("Generating Random keys...");

        for (int i = 0; i < 1000; i++) {
            service.submit(() -> {

                for (int j = 0; j < 1000; j++) {
                    String data = RandomStringUtils.randomAlphanumeric(25);
                    randomKeysGenerated.put(data, data);
                }

                latch.countDown();
                logger.debug("Current size of Random keys generated :" + randomKeysGenerated.size());
            });
        }

        latch.await();

        logger.info("Total size of Random keys generated :" + randomKeysGenerated.size());
        service.shutdown();

        return randomKeysGenerated;
    }

    private Map<String, String> getReadRows() throws InterruptedException {
        Map<String, String> keysToUseForQuery = new ConcurrentHashMap<>();

        if (currentTokenRanges == null) {
            logger.info("Fatal Initialization error: TokenRanges are not initialized");
            return keysToUseForQuery;
        }

        ExecutorService service = Executors.newScheduledThreadPool(100);
        CountDownLatch latch = new CountDownLatch(currentTokenRanges.size());
        keysToUseForQuery.clear();

        List<Long> currentTokenRangesShuffled = new ArrayList<>(currentTokenRanges.values());
        Collections.shuffle(currentTokenRangesShuffled, getRandom());

        for (long tokenStart : currentTokenRangesShuffled) {
            service.submit(() -> {
                String query = String.format("SELECT %s from %s.%s where token (%s) > %d and token(%s) < %d limit 100",
                        getPrimaryKey(),
                        keyspace,
                        tableName,
                        getPrimaryKey(),
                        tokenStart,
                        getPrimaryKey(),
                        tokenStart + Constants.getTokenChunkSize());

                System.out.println(query);

                boolean retrievalSucceeded = false;
                int numOfAttempts = 3;

                while (!retrievalSucceeded && numOfAttempts < Constants.getNumOfRetriesCassandra()) {
                    try {
                        ResultSet results = getCurrentSession().execute(query);
                        retrievalSucceeded = true;

                        for (Row row : results.all()) {
                            String data = row.getString(0);
                            keysToUseForQuery.put(data, data);

                            System.out.println(data);
                        }
                    } catch (Exception ex) {
                        logger.error("Unable to retrieve Keys from TokenRange: {} <=> {} with Exception: {}", tokenStart, tokenStart + Constants.getTokenChunkSize(), ex);

                        runExponentialBackoff(numOfAttempts);
                        numOfAttempts++;
                    }
                }

                latch.countDown();
                logger.debug("Current set of retrieved keys :" + keysToUseForQuery.size());
           });
        }

        latch.await();

        logger.info("Read completed with rows:" + keysToUseForQuery.size());

        service.shutdown();

        return keysToUseForQuery;
    }

    private void generateTokenRanges() {
        long tokenStart = Long.MIN_VALUE;
        long tokenEnd = Long.MIN_VALUE;
        int index = 0;

        while (tokenEnd < Long.MAX_VALUE) {
            if (Long.MAX_VALUE - Constants.getTokenChunkSize() > tokenStart) {
                tokenEnd = tokenStart + Constants.getTokenChunkSize();
            } else {
                tokenEnd = Long.MAX_VALUE;
            }

            currentTokenRanges.put(index, tokenStart);
            index++;

            // Update the tokenStart value
            tokenStart = (tokenEnd != Long.MAX_VALUE) ? tokenEnd + 1 : Long.MAX_VALUE;
        }
    }

    public Session connect(String givenAddress, String givenUserName, String givenKey) throws Exception{

        try {

            final KeyStore keyStore = KeyStore.getInstance("JKS");
            File sslKeyStoreFile = null;
            String sslKeyStorePass = "changeit";

            String ssl_keystore_file_path = null;
            String ssl_keystore_password = null;

            // If ssl_keystore_file_path, build the path using JAVA_HOME directory.
            if (ssl_keystore_file_path == null || ssl_keystore_file_path.isEmpty()) {
                String javaHomeDirectory = System.getenv("JAVA_HOME");
                if (javaHomeDirectory == null || javaHomeDirectory.isEmpty()) {
                    throw new Exception("JAVA_HOME not set");
                }
                ssl_keystore_file_path = new StringBuilder(javaHomeDirectory).append("/jre/lib/security/cacerts").toString();
            }

            sslKeyStorePass = (ssl_keystore_password != null && !ssl_keystore_password.isEmpty()) ?
                    ssl_keystore_password : sslKeyStorePass;

            sslKeyStoreFile = new File(ssl_keystore_file_path);

            try (final InputStream is = new FileInputStream(sslKeyStoreFile)) {
                keyStore.load(is, sslKeyStorePass.toCharArray());
            }

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            kmf.init(keyStore, sslKeyStorePass.toCharArray());
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                    .getDefaultAlgorithm());
            tmf.init(keyStore);

            // Creates a socket factory for HttpsURLConnection using JKS contents.
            final SSLContext sc = SSLContext.getInstance("TLSv1.2");
            sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new java.security.SecureRandom());

            JdkSSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sc)
                    .build();

            PoolingOptions poolOptions = new PoolingOptions();
            poolOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 128);
            poolOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 128);

            setCluster(Cluster
                    .builder()
                    .addContactPoint(givenAddress)
                    .withPort(10350)
                    .withCredentials(givenUserName, givenKey)
                    .withPoolingOptions(poolOptions)
                    .withSSL(sslOptions)
                    .build());

            return getCluster().connect();
        } catch (Exception ex) {
            logger.error("Fatal error: Unable to connect to Cassandra with exception: {}", ex);

            throw ex;
        }
    }

    private Map<String, String> getKeysForLoadTest() throws Exception {
        Map<String, String> keysToQuery = new ConcurrentHashMap<>();

        if (Constants.getReadMode().equals(mode)) {
            keysToQuery = getReadRows();
        } else if (Constants.getWriteMode().equals(mode)) {
            keysToQuery = generateRowKeys();
        }

        return keysToQuery;
    }
}
