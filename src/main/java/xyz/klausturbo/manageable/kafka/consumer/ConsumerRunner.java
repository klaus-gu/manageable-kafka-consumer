package xyz.klausturbo.manageable.kafka.consumer;//package com.ovopark.smart.offset.commit.kafka.consumer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * 消费者内的线程管理 . todo consumer 组内的 consumer 线程的监控
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class ConsumerRunner<K, V> {
    
    /**
     * consumer thread group.
     */
    private final ThreadGroup consumerThreadGroup;
    
    /**
     * Inner ScheduledThreadPoolExecutor to check consumer thread state .
     **/
    private final ScheduledThreadPoolExecutor threadStateMonitor;
    
    private final OffsetManageableKafkaConsumer<K, V> kafkaConsumer;
    
    /**
     * @param groupName
     * @param kafkaConsumerProperties
     * @param offsetMonitorPageSize
     * @param offsetMonitorMaxOpenPagesPerPartition
     * @param maxQueuedRecords
     */
    public ConsumerRunner(String groupName, Properties kafkaConsumerProperties, int offsetMonitorPageSize,
            int offsetMonitorMaxOpenPagesPerPartition, int maxQueuedRecords) {
        this.consumerThreadGroup = new ThreadGroup(groupName);
        this.threadStateMonitor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "consumer-thread-monitor");
                thread.setDaemon(true);
                return thread;
            }
        });
        this.kafkaConsumer = new OffsetManageableKafkaConsumer<>(kafkaConsumerProperties, offsetMonitorPageSize,
                offsetMonitorMaxOpenPagesPerPartition, maxQueuedRecords);
        
    }
    
    public void run() throws IOException, InterruptedException {
        kafkaConsumer.start();
        
    }
    
    
}
