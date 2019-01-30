package com.microsoft.cosmos.cassandrastress;

import java.io.*;
import java.security.Key;
import java.security.KeyStore;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import com.google.common.base.StandardSystemProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.HttpConstants;

import java.sql.Timestamp;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import com.microsoft.applicationinsights.TelemetryConfiguration;
import com.microsoft.applicationinsights.TelemetryClient;

import com.datastax.driver.core.*;
import static com.datastax.driver.mapping.Mapper.Option.*;


public class CassandraStress
{
    private final JCommander jCommander;
    @Parameter
    private List<String> parameters = new ArrayList<>();

    @Parameter(
            names = {"--endpoint", "-e" },
            description = "Cosmos DB Endpoint"
    )
    private String accountUrl = "";

    @Parameter(
            names = {"--key", "-k" },
            description = "Cosmos DB Key"
    )
    private String accountKey =  "";

    @Parameter(
            names = {"--username", "-user" },
            description = "Cassandra UserName"
    )
    private String userName = "cassandra";

    @Parameter(
            names = {"--keyspace", "-ks" },
            description = "Cosmos DB Database"
    )
    private String keyspace ="";// "cluster";

    @Parameter(
            names = {"--tablename", "-table" },
            description = "Cosmos DB Collection"
    )
    private String tableName = "";

    @Parameter(
            names = {"--help" },
            help = true,
            description = "Usage"
    )
    private boolean help = false;

    @Parameter(
            names = {"--operation", "-op" },
            description = "Operation Name"
    )
    private String operationName = "run_load_test";

    @Parameter(
            names = {"--mode", "-mode" },
            description = "Read/Write mode"
    )
    private String mode = "read_mode";

    public CassandraStress(String[] args) {
        jCommander = new JCommander(this, args);
    }

    public Cluster cluster;

    public Session currentSession;

    Map<Integer, Long> currentLeases = new HashMap<>();

    Map<String, String> KeysToQuery = new ConcurrentHashMap<String, String>();

    Map<UUID, Long> LatenciesMap = new ConcurrentHashMap<>();

    public static long tokenChunkSize = TenPower(15);

    public static String primaryKey = "product_id";// "doc_id";

    public static int NumOfRetriesCassandra = 12;

    public static int localConcurrentSetSize = 1000;

    public static int MaxAttemptsOnThrottle = 9;

    public static Random random = new Random();

    public static String ReadMode = "read_mode";

    public static String WriteMode = "write_mode";

    public static String UpsertMode = "upsert_mode";

    public static int PowerOfTwo(int power)
    {
        if (power <= 0)
        {
            return 1;
        }

        // Ceiling for the return value
        if (power > 30)
        {
            return PowerOfTwo(30);
        }

        int result = 1;
        for (int i = power; i > 0; i--)
        {
            result *= 2;
        }

        return result;
    }

    public static long TenPower(int num)
    {
        long value = 1;
        for (int i = 0; i < num; i++)
        {
            value = value * 10;
        }

        return value;
    }

    public static void ExponentialBackoff(int numOfAttempts) throws InterruptedException
    {
        // Ceiling for Exponential backoff
        if (numOfAttempts > NumOfRetriesCassandra)
        {
            numOfAttempts = NumOfRetriesCassandra;
        }

        int waitTimeInMilliSeconds = PowerOfTwo(numOfAttempts) * 100;
        System.out.format("Exponential backoff: Retrying after Waiting for %d milliseconds", waitTimeInMilliSeconds);
        Thread.sleep(waitTimeInMilliSeconds);
        numOfAttempts++;
    }

    public static void main(String[] args) throws Exception, InterruptedException
    {
        System.out.println("CassandraStress testing...");

        CassandraStress stressReader = new CassandraStress(args);
        if (stressReader.help)
        {
            stressReader.jCommander.usage();
            return;
        }

        if (!WriteMode.equals(stressReader.mode) &&
                !ReadMode.equals(stressReader.mode) &&
                !UpsertMode.equals(stressReader.mode))
        {
            System.out.println("Mode is neither Read nor Write: " + stressReader.mode);
            stressReader.jCommander.usage();
            return;
        }

        // initialize Leases
        stressReader.generateLeases();

        switch (stressReader.operationName) {
            case "get_ids":
                stressReader.getIds();
                break;
            case "run_load_test":
                stressReader.runLoadTest();
                break;
            default:
                break;
        }

        // end of program
        System.exit(0);
    }

    private void runLoadTest() throws InterruptedException
    {
        // Add jitter before starting loadrun
        Thread.sleep(random.nextInt(1000));

        // Telemetry Client
        TelemetryClient telemetry = new TelemetryClient();
        telemetry.getContext().getUser().setId("cosmos");
        telemetry.getContext().getDevice().setOperatingSystem(StandardSystemProperty.OS_NAME.value());
        telemetry.getContext().getDevice().setOperatingSystemVersion(StandardSystemProperty.OS_VERSION.value());
        TelemetryConfiguration.getActive().setInstrumentationKey("db41ac6a-45ea-4d42-95bd-bb1cd2075d35");

        ExecutorService service = Executors.newScheduledThreadPool(100);

        List<String> keysToQueryShuffled = new ArrayList<>(KeysToQuery.values());
        Collections.shuffle(keysToQueryShuffled, random);

        // Remove the second half of list for randomizing calls for loadtest purposes
        int listSize = keysToQueryShuffled.size();
        keysToQueryShuffled.subList(listSize/4, listSize*3/4);

        Map<Integer, String> localSetForKeys = new ConcurrentHashMap<>();
        int index = 0;

        MappingManager manager = new MappingManager(currentSession);
        Mapper<raw_price_data> mapper_raw_price_data = manager.mapper(raw_price_data.class);
        Mapper<daily_max_price_data> mapper_daily_max_price_data = manager.mapper(daily_max_price_data.class);

        for (String rowKey : keysToQueryShuffled )
        {
            localSetForKeys.put(index, rowKey);
            index++;

            if (index == localConcurrentSetSize)
            {
                CountDownLatch latch = new CountDownLatch(localConcurrentSetSize);

                System.out.println("localSetForKeys" + localSetForKeys.size());
                System.out.println("index" + index);

                for (String rowKey1 : localSetForKeys.values())
                {
                    service.submit(() ->
                    {
                        StopWatch stopwatch = StopWatch.createStarted();

                        Boolean needRetry = true;
                        int numOfAttempts = 0;

                        while (numOfAttempts < MaxAttemptsOnThrottle && needRetry)
                        {
                            try
                            {
                                if (ReadMode.equals(mode))
                                {
                                    mapper_raw_price_data.get(rowKey1);
                                    //mapper_daily_max_price_data.get(rowKey1);
                                }
                                else if (WriteMode.equals(mode))
                                {
                                    raw_price_data data_raw = RandomData.GetNew_raw_price_data();
                                    daily_max_price_data data_max = RandomData.GetNew_daily_max_price_data();

                                    data_max.product_id = data_raw.product_id;
                                    data_max.warehouse_id = data_raw.warehouse_id;

                                    //System.out.println(data_max.timestamp);
                                    // Truncate the Date to day
                                    Calendar cal = Calendar.getInstance();
                                    cal.setTime(data_max.timestamp);
                                    cal.set(Calendar.HOUR_OF_DAY, 0);
                                    cal.set(Calendar.MINUTE, 0);
                                    cal.set(Calendar.SECOND, 0);
                                    cal.set(Calendar.MILLISECOND, 0);
                                    data_max.timestamp = cal.getTime();

                                    //System.out.println("Truncated: " + data_max.timestamp);

                                    mapper_raw_price_data.save(data_raw);
                                    mapper_daily_max_price_data.save(
                                            data_max,
                                            timestamp(Double.doubleToLongBits(data_raw.price * 10000)));
                                }
                                else if (UpsertMode.equals(mode))
                                {
                                    // largeschema data = RandomData.GetNewDoc();
                                    raw_price_data data_raw = RandomData.GetNew_raw_price_data();
                                    daily_max_price_data data_max = RandomData.GetNew_daily_max_price_data();

                                    // data.doc_id = rowKey1;
                                    data_raw.product_id = rowKey1;
                                    data_max.product_id = rowKey1;
                                    data_max.warehouse_id = data_raw.warehouse_id;

                                    mapper_raw_price_data.save(data_raw);
                                    mapper_daily_max_price_data.save(
                                            data_max,
                                            timestamp(Double.doubleToLongBits(data_raw.price * 10000)));
                                }

                                needRetry = false;
                            }
                            catch (OverloadedException throttle)
                            {
                                numOfAttempts++;
                                System.out.println("I've seen a thrttle");
                            }
                            catch (IllegalStateException e)
                            {
                                numOfAttempts++;
                                Boolean IsThrottled = HandleThrottle(e);

                                if (IsThrottled)
                                {
                                    needRetry = true;

                                    // Exponential Backoff
                                    try
                                    {
                                        ExponentialBackoff(numOfAttempts);
                                        //numOfAttempts++;
                                    }
                                    catch(Exception jex)
                                    {
                                        jex.printStackTrace();
                                    }
                                }
                            }
                            catch (Exception unknownException)
                            {
                                System.out.println("Had hit Unknown exception");
                                unknownException.printStackTrace();

                                // Exponential Backoff
                                try
                                {
                                    ExponentialBackoff(numOfAttempts);
                                    numOfAttempts++;
                                }
                                catch(Exception jex)
                                {
                                    jex.printStackTrace();
                                }

                                // throw unknownException;
                            }
                        } // while

                        long timeConsumed = stopwatch.getTime(TimeUnit.MILLISECONDS);
                        stopwatch.stop();

                        LatenciesMap.put(UUID.randomUUID(), timeConsumed);

                        latch.countDown();
                    });
                }

                System.out.println("===========================================About to wait for the batch to complete: " + new Timestamp(System.currentTimeMillis()));
                // Wait for the local set of reads to complete
                if (latch != null)
                {
                    latch.await(10, TimeUnit.SECONDS);
                }
                System.out.println("===========================================Wait for the batch completed: " + new Timestamp(System.currentTimeMillis()));

                // Parse the latencies
                System.out.println("Number of requests succeeded: " + LatenciesMap.size());
                Long sumOfLatencies = 0L;
                for (Long val: LatenciesMap.values())
                {
                    sumOfLatencies += val;
                }

                Long avgLatency = sumOfLatencies/LatenciesMap.size();

                if (ReadMode.equals(mode))
                {
                    System.out.println("Avg Latency: " +  avgLatency);
                    telemetry.trackMetric("AvgLatency", avgLatency);
                }
                else if (WriteMode.equals(mode))
                {
                    System.out.println("Writes Avg Latency: " +  avgLatency);
                    telemetry.trackMetric("WriteAvgLatency", avgLatency);
                }
                else if (UpsertMode.equals(mode))
                {
                    System.out.println("Upsert Avg Latency: " +  avgLatency);
                    telemetry.trackMetric("UpsertAvgLatency", avgLatency);
                }

                // Calculate p50/p70/p90/p99
                ArrayList<Long> dataSet = new ArrayList<>(LatenciesMap.values());
                int datasetSize = dataSet.size();

                Collections.sort(dataSet, Collections.reverseOrder());

                Long p50 = dataSet.get(datasetSize/2);
                if (ReadMode.equals(mode))
                {
                    System.out.println("p50 Latency: " +  p50);
                    telemetry.trackMetric("p50Latency", p50);
                }
                else if (WriteMode.equals(mode))
                {
                    System.out.println("Writes p50 Latency: " +  p50);
                    telemetry.trackMetric("Writep50Latency", p50);
                }
                else if (UpsertMode.equals(mode))
                {
                    System.out.println("Upsert p50 Latency: " +  p50);
                    telemetry.trackMetric("Upsertp50Latency", p50);
                }

                Long p90 = dataSet.get(datasetSize/10);
                if (ReadMode.equals(mode))
                {
                    System.out.println("p90 Latency: " +  p90);
                    telemetry.trackMetric("p90Latency", p90);
                }
                else if (WriteMode.equals(mode))
                {
                    System.out.println("Writes p90 Latency: " +  p90);
                    telemetry.trackMetric("Writep90Latency", p90);
                }
                else if (UpsertMode.equals(mode))
                {
                    System.out.println("Upsert p90 Latency: " +  p90);
                    telemetry.trackMetric("Upsertp90Latency", p90);
                }

                Long p99 = dataSet.get(datasetSize/100);
                if (ReadMode.equals(mode))
                {
                    System.out.println("p99 Latency: " +  p99);
                    telemetry.trackMetric("p99Latency", p99);
                }
                else if (WriteMode.equals(mode))
                {
                    System.out.println("Writes p99 Latency: " +  p99);
                    telemetry.trackMetric("Writep99Latency", p99);
                }
                else if (UpsertMode.equals(mode))
                {
                    System.out.println("Upsert p99 Latency: " +  p99);
                    telemetry.trackMetric("Upsertp99Latency", p99);
                }

                // Cleanup
                LatenciesMap.clear();
                localSetForKeys.clear();
                index = 0;
            }
        }
    }

    private Boolean HandleThrottle(IllegalStateException e)
    {
        Throwable exception = e;
        Boolean isThrottled = false;
        while (!(exception instanceof DocumentClientException) && exception != null)
        {
            exception = e.getCause();
        }

        if (exception != null)
        {
            DocumentClientException documentClientException = (DocumentClientException) exception;
            if (documentClientException.getStatusCode() == HttpConstants.StatusCodes.TOO_MANY_REQUESTS)
            {
                System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$ Had hit throttle----------");
                isThrottled = true;
            }
            else
            {
                // e.printStackTrace();
                System.out.println(e.getCause().toString());
            }
        }

        return isThrottled;
    }

    private void connectToCassandra()
    {
        currentSession = connect(accountUrl, userName, accountKey);
    }

    private void generateRowKeys() throws InterruptedException
    {
        ExecutorService service = Executors.newScheduledThreadPool(100);
        CountDownLatch latch = new CountDownLatch(1000);
        KeysToQuery.clear();

        for (int i = 0; i < 1000; i++)
        {
            service.submit(() -> {

                for (int j = 0; j < 10000; j++)
                {
                    String data = RandomData.GetRandomString(25);
                    KeysToQuery.put(data, data);
                }

                latch.countDown();
                System.out.println("======================== " + KeysToQuery.size());
            });
        }

        latch.await();
        System.out.println("Read completed with rows:" + KeysToQuery.size());
        service.shutdown();
    }

    private void getReadRows() throws InterruptedException
    {
        if (currentLeases == null)
        {
            System.out.println("Fatal Initialization error: Leases are not initialized");
            return;
        }

        ExecutorService service = Executors.newScheduledThreadPool(100);
        CountDownLatch latch = new CountDownLatch(currentLeases.size());
        KeysToQuery.clear();

        List<Long> currentLeasesShuffled = new ArrayList<>(currentLeases.values());
        Collections.shuffle(currentLeasesShuffled, random);

        for (long tokenStart: currentLeasesShuffled)
        {
            service.submit(() -> {
            String query = String.format("SELECT %s from %s.%s where token (%s) > %d and token(%s) < %d limit 100",
                    primaryKey,
                    keyspace,
                    tableName,
                    primaryKey,
                    tokenStart,
                    primaryKey,
                    tokenStart + tokenChunkSize);

            boolean retrievalSucceeded = false;
            int numOfAttempts = 3;

            while (!retrievalSucceeded && numOfAttempts < NumOfRetriesCassandra) {
                try {
                    ResultSet results = currentSession.execute(query);
                    retrievalSucceeded = true;

                    for (Row row : results.all()) {
                        String data = row.getString(0);
                        KeysToQuery.put(data, data);
                    }
                } catch (Exception ex) {
                    String error = String.format("Unable to retrieve Keys from TokenRange: %d <=> %d", tokenStart, tokenStart + tokenChunkSize);
                    System.out.println(error);
                    ex.printStackTrace();

                    // Exponential Backoff
                    try
                    {
                        ExponentialBackoff(numOfAttempts);
                        numOfAttempts++;
                    }
                    catch(Exception jex)
                    {
                        jex.printStackTrace();
                    }
                }
            }

            latch.countDown();
                System.out.println("======================== " + KeysToQuery.size());
            });
        }

        latch.await();
        System.out.println("Read completed with rows:" + KeysToQuery.size());
        service.shutdown();
    }

    private void generateLeases()
    {
        long tokenStart = Long.MIN_VALUE;
        long tokenEnd = Long.MIN_VALUE;
        int index = 0;

        while (tokenEnd < Long.MAX_VALUE)
        {
            if (Long.MAX_VALUE - tokenChunkSize > tokenStart)
            {
                tokenEnd = tokenStart + tokenChunkSize;
            }
            else
            {
                tokenEnd = Long.MAX_VALUE;
            }

            currentLeases.put(index, tokenStart);
            index++;

            // Update the tokenStart value
            tokenStart = (tokenEnd != Long.MAX_VALUE) ? tokenEnd + 1 : Long.MAX_VALUE;
        }
    }

    public Session connect(String givenAddress, String givenUserName, String givenKey) {

        try {

            final KeyStore keyStore = KeyStore.getInstance("JKS");
            File sslKeyStoreFile = null;
            String sslKeyStorePassword = "changeit";

            //
            String ssl_keystore_file_path = null;
            String ssl_keystore_password = null;

            // If ssl_keystore_file_path, build the path using JAVA_HOME directory.
            if (ssl_keystore_file_path == null || ssl_keystore_file_path.isEmpty()) {
                String javaHomeDirectory = System.getenv("JAVA_HOME");
                if (javaHomeDirectory == null || javaHomeDirectory.isEmpty()) {
                    throw new Exception("JAVA_HOME not set");
                }
                ssl_keystore_file_path = new StringBuilder(javaHomeDirectory).append("/lib/security/cacerts").toString();
            }

            sslKeyStorePassword = (ssl_keystore_password != null && !ssl_keystore_password.isEmpty()) ?
                    ssl_keystore_password : sslKeyStorePassword;

            sslKeyStoreFile = new File(ssl_keystore_file_path);

            //
            try (final InputStream is = new FileInputStream(sslKeyStoreFile)) {
                keyStore.load(is, sslKeyStorePassword.toCharArray());
            }

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            kmf.init(keyStore, sslKeyStorePassword.toCharArray());
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

            cluster = Cluster
                    .builder()
                    .addContactPoint(givenAddress)
                    .withPort(10350)
                    .withCredentials(givenUserName, givenKey)
                    .withPoolingOptions(poolOptions)
                    .withSSL(sslOptions)
                    .build();
            return cluster.connect();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

        return null;
    }

    private void getIds() throws InterruptedException
    {
        connectToCassandra();

        // Test
        ////////////////////////////////////////////
        /*MappingManager manager = new MappingManager(currentSession);
        Mapper<largeschema> mapper = manager.mapper(largeschema.class);

        int j = 10;
        while(j == 10)
        {
            System.out.println(RandomData.GetNewDoc().doc_id);
            mapper.save(RandomData.GetNewDoc());
        }*/

        ///////////////////////////////////////////

        for (int i = 0; i < 100; i++) {
            if (ReadMode.equals(mode) || UpsertMode.equals(mode)) {
                getReadRows();
            } else if (WriteMode.equals(mode)) {
                generateRowKeys();
            }

            // Add jitter before starting loadrun
            Thread.sleep(random.nextInt(10000));

            runLoadTest();
        }
    }
}
