package xyz.klausturbo.manageable.kafka.consumer;//package com.ovopark.smart.offset.commit.kafka.consumer;
//
//import java.util.Properties;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.TimeUnit;
//
///**
// * Consumer Thread Manager . todo consumer 组内的 consumer 线程的监控
// *
// * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
// * @program manageable-kafka-consumer
// **/
//public class ConsumerManager<K, V> {
//
//    /**
//     * init size for Kafka consumer.
//     */
//    private final int initConsumerGroupSize;
//
//    /**
//     * consumer thread group.
//     */
//    private final ThreadGroup consumerThreadGroup;
//
//    /**
//     * .
//     **/
//    private final ConcurrentHashMap<String, OffsetManageableKafkaConsumer> applyedConsumerThreadMap;
//
//    /**
//     * Inner ScheduledThreadPoolExecutor to check consumer thread state .
//     **/
//    private final ScheduledThreadPoolExecutor threadStateMonitor;
//
//    /**
//     * ${@link ConsumerAbilityCoordinator}.
//     */
//    private final ConsumerAbilityCoordinator abilityCoordinator;
//
//    /**
//     * Constructor.
//     *
//     * @param groupName             consumer threadgroup name，make it easy to manage consumer thread
//     * @param initConsumerGroupSize number of ${@link OffsetManageableKafkaConsumer} will be created
//     */
//    public ConsumerManager(String groupName, Integer initConsumerGroupSize) {
//        this.initConsumerGroupSize = initConsumerGroupSize;
//        this.consumerThreadGroup = new ThreadGroup(groupName);
//        this.applyedConsumerThreadMap = new ConcurrentHashMap<>(initConsumerGroupSize);
//        this.abilityCoordinator = new ConsumerAbilityCoordinator();
//        this.threadStateMonitor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
//            @Override
//            public Thread newThread(Runnable r) {
//                Thread thread = new Thread(r, "consumer-thread-monitor");
//                thread.setDaemon(true);
//                return thread;
//            }
//        });
//
//    }
//
//    public synchronized void run(Properties kafkaConsumerProperties) {
//        run(kafkaConsumerProperties, 10_000, 1000, 10_000);
//    }
//
//    public synchronized void run(Properties kafkaConsumerProperties, int offsetMonitorPageSize,
//            int offsetMonitorMaxOpenPagesPerPartition, int maxQueuedRecords) {
//        monitor();
//        for (int i = 1; i <= initConsumerGroupSize; i++) {
//            OffsetManageableKafkaConsumer<K, V> consumer = new OffsetManageableKafkaConsumer<>(kafkaConsumerProperties,
//                    offsetMonitorPageSize, offsetMonitorMaxOpenPagesPerPartition, maxQueuedRecords);
//            consumer.consume();
//            applyedConsumerThreadMap.put(consumer.getThread().getName(), consumer);
//        }
//    }
//
//    private void monitor() {
//
//        threadStateMonitor.scheduleAtFixedRate(() -> applyedConsumerThreadMap.forEach((key, value) -> {
//            Thread worker = value.getThread();
//            System.out.println("线程健康检查..." + worker.getState());
//            if (!worker.isAlive() || worker.isInterrupted()) {
//                // todo 发送企业微信消息
//            }
//            if (worker.getState() != Thread.State.RUNNABLE) {
//                // todo 发送企业微信消息
//            }
//        }), 30, 10, TimeUnit.SECONDS);
//
//    }
//
//}
