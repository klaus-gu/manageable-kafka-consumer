package xyz.klausturbo.manageable.kafka.consumer;

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
import org.slf4j.Logger;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

/**
 * ${@link KafkaConsumer} A KafkaConsumer manage all workers.
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program dc-log-collect
 **/
public class OffsetManageableKafkaConsumer<K, V> implements Closeable {
    
    private static final Logger LOGGER = LogUtil.getLogger(OffsetManageableKafkaConsumer.class);
    
    private static final int POLL_TIMEOUT_MILLIS = 10;
    
    private final KafkaConsumer<K, V> kafkaConsumer;
    
    private final Thread thread;
    
    private final OffsetMonitor offsetMonitor;
    
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
    
    private int maxPollIntervalMillis = 300000;
    
    private long lastPollTime;
    
    private ConsumerRebalanceListener rebalanceListener;
    
    private String topic;
    
    private volatile boolean stop = false;
    
    public OffsetManageableKafkaConsumer(Properties kafkaConsumerProperties) {
        this(kafkaConsumerProperties, 10_000, 1000, 10_000);
    }
    
    public OffsetManageableKafkaConsumer(Properties kafkaConsumerProperties, int offsetMonitorPageSize,
            int offsetMonitorMaxOpenPagesPerPartition, int maxQueuedRecords) {
        if (kafkaConsumerProperties.containsKey(MAX_POLL_INTERVAL_MS_CONFIG)) {
            maxPollIntervalMillis = Integer.parseInt(kafkaConsumerProperties.getProperty(MAX_POLL_INTERVAL_MS_CONFIG));
        } else {
            kafkaConsumerProperties.put(MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMillis);
        }
        kafkaConsumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
        // 初始化这个 consumer 最多缓存的 record 条数
        this.queuedRecords = new ArrayBlockingQueue<>(maxQueuedRecords);
        
        this.thread = initConsumerThread();
        this.offsetMonitor = new OffsetMonitor(offsetMonitorPageSize, offsetMonitorMaxOpenPagesPerPartition);
        
        offsetCommitCallback = (offsets, e) -> {
            if (e != null) {
                LOGGER.error("Failed to commit offset. It is valid just if Kafka is out of reach "
                        + "or it was in a re-balance process recently." + e);
            } else {
                LOGGER.info("[Offsets committed: {}].", offsets);
            }
        };
        
        this.unappliedAcks = new LinkedBlockingQueue<>();
        
        this.assignedPartitions = new ArrayList<>();
        
        this.internalRebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    LOGGER.info("Kafka consumer previous assignment revoked:" + partitions);
                }
                if (rebalanceListener != null) {
                    rebalanceListener.onPartitionsRevoked(partitions);
                }
            }
            
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                assignedPartitions.clear();
                assignedPartitions.addAll(partitions);
                LOGGER.info("Kafka consumer partitions assigned: " + partitions);
                offsetMonitor.reset();
                if (rebalanceListener != null) {
                    rebalanceListener.onPartitionsAssigned(partitions);
                }
            }
        };
    }
    
    public void subscribe(String topic) {
        subscribe(topic, null);
    }
    
    public void subscribe(String topic, ConsumerRebalanceListener rebalanceListener) {
        this.rebalanceListener = rebalanceListener;
        kafkaConsumer.subscribe(Collections.singleton(topic), internalRebalanceListener);
        this.topic = topic;
        thread.setName("kafka-consumer-for-" + topic);
    }
    
    /**
     * Ack one handled offset.
     **/
    public void ack(PartitionOffset partitionOffset) {
        unappliedAcks.add(partitionOffset);
    }
    
    public void start() throws InterruptedException, IOException {
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
    }
    
    public ConsumerRecord<K, V> poll() {
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
    
    /**
     * init a thread ,polling messages from kafka .
     * <p>
     * 1. handle offset acknowledge.
     * <p>
     * 2. put message into inner queue <queuedRecords>.
     * @return
     */
    private Thread initConsumerThread() {
        return new Thread(() -> {
            while (!stop) {
                try {
                    handleAcks();
                    lastPollTime = System.currentTimeMillis();
                    putRecordsInQueue(kafkaConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MILLIS)));
                } catch (WakeupException | InterruptException | InterruptedException e) {
                    if (!stop) {
                        throw new IllegalStateException("Unexpected interrupt.");
                    }
                    LOGGER.error(Thread.currentThread().getName() + " interrupted.");
                    return;
                }
            }
        });
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
                LOGGER.warn("Offset monitor for partition " + record.partition() + " is full. "
                        + "Waiting... [You should never see this message. Consider increasing "
                        + "the max number of open pages]");
                handleAcks();
                Thread.sleep(1);
                keepConnectionAlive();
            }
            // put message into queue first.
            while (!queuedRecords.offer(record)) {
                handleAcks();
                Thread.sleep(1);
                keepConnectionAlive();
            }
        }
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
}
