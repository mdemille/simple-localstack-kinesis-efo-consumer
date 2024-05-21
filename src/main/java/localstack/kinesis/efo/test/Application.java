package localstack.kinesis.efo.test;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.metrics.LoggingMetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class Application {

  private static final Logger log = LoggerFactory.getLogger(Application.class);


  public static void main(String... args) {
    new Application().run();
  }

  private final String streamName = "efo-test";
  private final Region region;
  private final KinesisAsyncClient kinesisClient;
  private final AwsCredentialsProvider credentialsProvider;

  private Application() {
    this.region = Region.of("us-east-1");
    this.credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
    this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()
                                                                      .credentialsProvider(credentialsProvider)
                                                                      .endpointOverride(URI.create("http://localhost:4566"))
                                                                      .overrideConfiguration(builder -> builder
                                                                        .apiCallAttemptTimeout(Duration.ofMinutes(1))
                                                                        .apiCallTimeout(Duration.ofMinutes(1))
                                                                        .metricPublishers(List.of(LoggingMetricPublisher.create())))
                                                                      .region(this.region));
  }

  private void run() {
    ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<?> producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 10, 1, TimeUnit.SECONDS);

    DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder()
      .credentialsProvider(credentialsProvider)
      .endpointOverride(URI.create("http://localhost:4566"))
      .region(region)
      .build();
    CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder()
      .credentialsProvider(credentialsProvider)
      .endpointOverride(URI.create("http://localhost:4566"))
      .region(region)
      .build();
    ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, streamName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new SampleRecordProcessorFactory());

    Scheduler scheduler = new Scheduler(
      configsBuilder.checkpointConfig(),
      configsBuilder.coordinatorConfig(),
      configsBuilder.leaseManagementConfig(),
      configsBuilder.lifecycleConfig(),
      configsBuilder.metricsConfig(),
      configsBuilder.processorConfig(),
      configsBuilder.retrievalConfig() // Default behavior is EFO
        //.retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient)) // Uncomment this retrievalSpecificConfig to use polling
    );

    Thread schedulerThread = new Thread(scheduler);
    schedulerThread.setDaemon(true);
    schedulerThread.start();

    log.info("Press enter to shutdown");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    try {
      reader.readLine();
    } catch (IOException ioex) {
      log.error("Caught exception while waiting for confirm. Shutting down.", ioex);
    }

    log.info("Cancelling producer, and shutting down executor.");
    producerFuture.cancel(true);
    producerExecutor.shutdownNow();

    Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
    log.info("Waiting up to 20 seconds for shutdown to complete.");
    try {
      gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.info("Interrupted while waiting for graceful shutdown. Continuing.");
    } catch (ExecutionException e) {
      log.error("Exception while executing graceful shutdown.", e);
    } catch (TimeoutException e) {
      log.error("Timeout while waiting for shutdown. Scheduler may not have exited.");
    }
    log.info("Completed, shutting down now.");
  }

  private void publishRecord() {
    log.info("Publishing record");
    PutRecordRequest request = PutRecordRequest.builder()
      .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
      .streamName(streamName)
      .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(10)))
      .build();
    try {
      kinesisClient.putRecord(request).get();
    } catch (InterruptedException e) {
      log.info("Interrupted, assuming shutdown.");
    } catch (ExecutionException e) {
      log.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
    }
  }

  private static class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {
    public ShardRecordProcessor shardRecordProcessor() {
      return new SampleRecordProcessor();
    }
  }


  private static class SampleRecordProcessor implements ShardRecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(SampleRecordProcessor.class);

    private static final String SHARD_ID_MDC_KEY = "ShardId";

    private String shardId;

    public void initialize(InitializationInput initializationInput) {
      shardId = initializationInput.shardId();
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Processing {} record(s)", processRecordsInput.records().size());
        processRecordsInput.records()
          .forEach(r -> log.info("Processing record pk: {} -- Seq: {}", r.partitionKey(), r.sequenceNumber()));
      } catch (Throwable t) {
        log.error("Caught throwable while processing records. Aborting.");
        Runtime.getRuntime().halt(1);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void leaseLost(LeaseLostInput leaseLostInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Lost lease, so terminating.");
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void shardEnded(ShardEndedInput shardEndedInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Reached shard end checkpointing.");
        shardEndedInput.checkpointer().checkpoint();
      } catch (ShutdownException | InvalidStateException e) {
        log.error("Exception while checkpointing at shard end. Giving up.", e);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Scheduler is shutting down, checkpointing.");
        shutdownRequestedInput.checkpointer().checkpoint();
      } catch (ShutdownException | InvalidStateException e) {
        log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }
  }

}
