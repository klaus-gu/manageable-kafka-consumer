package xyz.klausturbo.manageable.kafka.consumer;//package com.ovopark.smart.offset.commit.kafka.consumer;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 消费者内的线程管理 .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class ConsumerRunner<K, V> {
    
    /**
     * Inner ScheduledThreadPoolExecutor to check consumer thread state .
     **/
    private final ScheduledThreadPoolExecutor threadStateMonitor;
    
    private final OffsetManageableKafkaConsumer<K, V> kafkaConsumer;
    
    private final ConcurrentHashMap<Integer/*partitionId*/, PartitionConsumeWorker<K, V>> partitionWorkers;
    
    private volatile boolean isRunning;
    
    /**
     * @param kafkaConsumerProperties
     * @param offsetMonitorPageSize
     * @param offsetMonitorMaxOpenPagesPerPartition
     * @param maxQueuedRecords
     */
    public ConsumerRunner(Properties kafkaConsumerProperties, int offsetMonitorPageSize,
            int offsetMonitorMaxOpenPagesPerPartition, int maxQueuedRecords) {
        this.threadStateMonitor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r, "kafka-consumer-thread-monitor");
            thread.setDaemon(true);
            return thread;
        });
        this.kafkaConsumer = new OffsetManageableKafkaConsumer<>(kafkaConsumerProperties, offsetMonitorPageSize,
                offsetMonitorMaxOpenPagesPerPartition, maxQueuedRecords);
        this.partitionWorkers = new ConcurrentHashMap<>();
        this.isRunning = true;
    }
    
    public synchronized void run(String topic) throws IOException, InterruptedException {
        List<Integer> partitions = Arrays.asList(new Integer[] {0, 1, 2, 3, 4, 5, 6, 7});
        kafkaConsumer.assign(topic, partitions).start();
        while (isRunning) {
            ConsumerRecord<K, V> consumerRecord = kafkaConsumer.poll();
            if (consumerRecord != null) {
                TopicPartition topicPartition = new TopicPartition(topic, consumerRecord.partition());
                PartitionConsumeWorker<K, V> worker = partitionWorkers.get(consumerRecord.partition());
                if (worker == null) {
                    worker = new PartitionConsumeWorker<K, V>(topicPartition, kafkaConsumer, r -> {
                        // 注册业务逻辑
                        return true;
                    },500,0.75);
                    partitionWorkers.put(consumerRecord.partition(), worker);
                }
               worker.offer(consumerRecord);
            } else {
                // 防止无消息的时候cpu空转.
                TimeUnit.MILLISECONDS.sleep(10);
            }
        }
    }
    
    public boolean stop() {
        this.isRunning = false;
        this.kafkaConsumer.close();
        return isRunning;
    }
}
