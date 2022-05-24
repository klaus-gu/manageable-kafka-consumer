package xyz.klausturbo.manageable.kafka.consumer;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import xyz.klausturbo.manageable.kafka.util.LogUtil;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

/**
 * ${@link KafkaConsumer} A KafkaConsumer manage all workers.
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program dc-log-collect
 **/
public class TurboKafkaConsumer<K, V> implements Closeable {
    
    private static final int POLL_TIMEOUT_MILLIS = 100;
    
    private final KafkaConsumer<K, V> kafkaConsumer;
    
    private final Thread thread;
    
    private final OffsetMonitor offsetMonitor;
    
    private final LoadingCache<String, TopicPartition> partitionLoadingCache = Caffeine.newBuilder()
            .expireAfterWrite(2000, TimeUnit.MILLISECONDS).build(new CacheLoader<String, TopicPartition>() {
                @Override
                public @Nullable TopicPartition load(@NonNull String s) throws Exception {
                    return null;
                }
            });
    
    /**
     * The queued records before delivering to the client, are kept here.
     */
    private final BlockingQueue<ConsumerRecord<K, V>> queuedRecords;
    
    /**
     * The acks which are not yet applied to the {@link #offsetMonitor} are kept in this queue.
     */
    private final BlockingQueue<PartitionOffset> unappliedAcks;
    
    private final OffsetCommitCallback offsetCommitCallback;
    
    private final ConsumerRebalanceListener internalRebalanceListener;
    
    private final List<TopicPartition> assignedPartitions;
    
    private final Map<Integer, PartitionConsumeWorker<K, V>> partitionWorkers = new HashMap<>();
    
    private final int maxQueuedRecords;
    
    private int maxPollIntervalMillis = 300000;
    
    private long lastPollTime;
    
    private ConsumerRebalanceListener rebalanceListener;
    
    private String topic;
    
    private Queue<Integer> partitionResumeWaitQueue = new LinkedBlockingQueue<>(1000);
    
    private volatile boolean stop = false;
    
    public TurboKafkaConsumer(Properties kafkaConsumerProperties) {
        this(kafkaConsumerProperties, 10_000, 1000, 10_000);
    }
    
    public TurboKafkaConsumer(Properties kafkaConsumerProperties, int offsetMonitorPageSize,
            int offsetMonitorMaxOpenPagesPerPartition, int maxQueuedRecords) {
        if (kafkaConsumerProperties.containsKey(MAX_POLL_INTERVAL_MS_CONFIG)) {
            maxPollIntervalMillis = Integer.parseInt(kafkaConsumerProperties.getProperty(MAX_POLL_INTERVAL_MS_CONFIG));
        } else {
            kafkaConsumerProperties.put(MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMillis);
        }
        kafkaConsumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.maxQueuedRecords = maxQueuedRecords;
        this.kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
        this.queuedRecords = new ArrayBlockingQueue<>(maxQueuedRecords);
        this.thread = initConsumerThread();
        this.offsetMonitor = new OffsetMonitor(offsetMonitorPageSize, offsetMonitorMaxOpenPagesPerPartition);
        offsetCommitCallback = (offsets, e) -> {
            if (e != null) {
                LogUtil.TURBO_KAFKA_CONSUMER.error("Failed to commit offset. It is valid just if Kafka is out of reach "
                        + "or it was in a re-balance process recently." + e);
            } else {
                LogUtil.TURBO_KAFKA_CONSUMER.info("[Offsets committed: {}].", offsets);
            }
        };
        
        this.unappliedAcks = new LinkedBlockingQueue<>();
        
        this.assignedPartitions = new ArrayList<>();
        
        this.internalRebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    LogUtil.TURBO_KAFKA_CONSUMER.info("Kafka consumer previous assignment revoked:" + partitions);
                }
                if (rebalanceListener != null) {
                    rebalanceListener.onPartitionsRevoked(partitions);
                }
            }
            
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                assignedPartitions.clear();
                assignedPartitions.addAll(partitions);
                LogUtil.TURBO_KAFKA_CONSUMER.info("Kafka consumer partitions assigned: " + partitions);
                offsetMonitor.reset();
                if (rebalanceListener != null) {
                    rebalanceListener.onPartitionsAssigned(partitions);
                }
            }
            
        };
        LogUtil.TURBO_KAFKA_CONSUMER.info("Init one kafkaConsumer.Properties:{}.", kafkaConsumerProperties.toString());
    }
    
//    public TurboKafkaConsumer<K, V> subscribe(String topic) {
//        subscribe(topic, null);
//        return this;
//    }
//
//    public TurboKafkaConsumer<K, V> subscribe(String topic, ConsumerRebalanceListener rebalanceListener) {
//        this.rebalanceListener = rebalanceListener;
//        kafkaConsumer.subscribe(Collections.singleton(topic), internalRebalanceListener);
//        this.topic = topic;
//        thread.setName("TurboKafkaConsumer-" + topic);
//        return this;
//    }
    
    public TurboKafkaConsumer<K, V> assign(String topic, List<Integer> partitions, Predicate<ConsumerRecord<K,V>> func) {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitions.forEach(p -> {
            TopicPartition topicPartition = new TopicPartition(topic, p);
            topicPartitions.add(topicPartition);
            PartitionConsumeWorker<K, V> partitionConsumeWorker = new PartitionConsumeWorker<K, V>(topicPartition, this,func
                   , maxQueuedRecords);
            partitionWorkers.put(topicPartition.partition(), partitionConsumeWorker);
        });
        kafkaConsumer.assign(topicPartitions);
        thread.setName("TurboKafkaConsumer-" + topic);
        this.topic = topic;
        return this;
    }
    
    /**
     * Ack one handled offset.
     **/
    protected void ack(PartitionOffset partitionOffset) {
        unappliedAcks.add(partitionOffset);
    }
    
    public TurboKafkaConsumer<K, V> start() throws InterruptedException, IOException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                kafkaConsumer.poll(Duration.ofMillis(0));
                Map<String, List<PartitionInfo>> topics = kafkaConsumer.listTopics();
                if (!topics.containsKey(topic)) {
                    throw new AssertionError("Subscribed topic does not exist in Kafka server.");
                }
            }).get(60, SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            kafkaConsumer.wakeup();
            throw new IOException("Failed connecting to Kafka.", e);
        } finally {
            executor.shutdown();
        }
        thread.start();
        return this;
    }
    
    protected ConsumerRecord<K, V> poll() {
        return queuedRecords.poll();
    }
    
    @Override
    public void close() {
        stop = true;
        kafkaConsumer.wakeup();
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new AssertionError("Unexpected interrupt.", e);
        }
        try {
            kafkaConsumer.close();
        } finally {
        }
    }
    
    private Thread initConsumerThread() {
        return new Thread(() -> {
            while (!stop) {
                try {
                    handleAcks();
                    resumePartition();
                    lastPollTime = System.currentTimeMillis();
                    putRecordsInQueue(kafkaConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MILLIS)));
                } catch (WakeupException | InterruptException | InterruptedException e) {
                    if (!stop) {
                        throw new IllegalStateException("Unexpected interrupt.");
                    }
                    LogUtil.TURBO_KAFKA_CONSUMER.error(Thread.currentThread().getName() + " interrupted.");
                    return;
                }
            }
        });
    }
    
    private void resumePartition() {
        Integer partitionId = partitionResumeWaitQueue.poll();
        if (partitionId != null) {
            LogUtil.TURBO_KAFKA_CONSUMER.info("【RESUME】Partition[{}] will be resumed .", partitionId);
            kafkaConsumer.resume(Collections.singletonList(new TopicPartition(topic, partitionId)));
        }
    }
    
    private void handleAcks() {
        int size = unappliedAcks.size();
        if (size == 0) {
            return;
        }
        List<PartitionOffset> offsets = new ArrayList<>(size);
        // put number=size element into offsets above.
        // number = size ,means all message.
        unappliedAcks.drainTo(offsets, size);
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (PartitionOffset partitionOffset : offsets) {
            OptionalLong offsetToCommit = offsetMonitor.ack(partitionOffset.partition(), partitionOffset.offset());
            if (offsetToCommit.isPresent()) {
                offsetsToCommit.put(new TopicPartition(topic, partitionOffset.partition()),
                        new OffsetAndMetadata(offsetToCommit.getAsLong()));
            }
        }
        if (!offsetsToCommit.isEmpty()) {
            kafkaConsumer.commitAsync(offsetsToCommit, offsetCommitCallback);
        }
    }
    
    private void putRecordsInQueue(ConsumerRecords<K, V> records) throws InterruptedException {
        for (ConsumerRecord<K, V> record : records) {
            while (!offsetMonitor.track(record.partition(), record.offset())) {
                LogUtil.TURBO_KAFKA_CONSUMER.warn("Offset monitor for partition " + record.partition() + " is full. "
                        + "Waiting... [You should never see this message. Consider increasing "
                        + "the max number of open pages]");
                handleAcks();
                Thread.sleep(1);
                keepConnectionAlive();
            }
            
            PartitionConsumeWorker<K, V> worker = partitionWorkers.get(record.partition());
            if (!worker.offer(record)) {
                handleAcks();
                Thread.sleep(1);
                keepConnectionAlive();
            }
            if (!worker.isPaused()) {
                LogUtil.TURBO_KAFKA_CONSUMER
                        .info("【PAUSE】Partition[{}] has been paused ，it will be resumed after worker consume all records.",
                                record.partition());
                worker.setPaused();
                kafkaConsumer.pause(Collections.singletonList(new TopicPartition(record.topic(), record.partition())));
            }
            //            while (!queuedRecords.offer(record)) {
            //                handleAcks();
            //                Thread.sleep(1);
            //                keepConnectionAlive();
            //            }
        }
        // 按分区分发消息，并暂停该分区的消费直到消费结束
        //        for (Map.Entry<TopicPartition, PartitionConsumeWorker<K, V>> workerEntry : partitionWorkers.entrySet()) {
        //            TopicPartition topicPartition = workerEntry.getKey();
        //            PartitionConsumeWorker<K, V> worker = workerEntry.getValue();
        //            List<ConsumerRecord<K, V>> partitionRecords = records.records(topicPartition);
        //            worker.offer(partitionRecords);
        //            LOGGER.info("Partition[{}] has been paused ，it will be resumed after worker consume all records.",
        //                    topicPartition.partition());
        //            worker.setPaused();
        //            kafkaConsumer.pause(Collections.singletonList(topicPartition));
        //        }
    }
    
    /**
     * maintain the connection .
     */
    private void keepConnectionAlive() {
        if (System.currentTimeMillis() - lastPollTime < (int) (0.7 * maxPollIntervalMillis)) {
            return;
        }
        boolean rebalanceHappened = true;
        while (rebalanceHappened) {
            List<TopicPartition> copyOfAssignedPartitions = new ArrayList<>(assignedPartitions);
            // stop consume.
            kafkaConsumer.pause(copyOfAssignedPartitions);
            rebalanceHappened = false;
            lastPollTime = System.currentTimeMillis();
            kafkaConsumer.poll(Duration.ofMillis(0));
        }
    }
    
    protected void requestResume(Integer partitionId) {
        if (!partitionResumeWaitQueue.contains(partitionId)) {
            partitionResumeWaitQueue.offer(partitionId);
        }
    }
}
