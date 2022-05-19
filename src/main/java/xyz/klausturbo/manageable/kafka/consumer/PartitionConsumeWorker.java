package xyz.klausturbo.manageable.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import xyz.klausturbo.manageable.kafka.storage.LocalOffsetStorage;
import xyz.klausturbo.manageable.kafka.util.LogUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Worker for single partition .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class PartitionConsumeWorker<K, V> implements Closeable {
    
    private final BlockingQueue<ConsumerRecord<K, V>> partitionQueue;
    
    /**
     * 分区消费者关注的分区信息
     */
    private final TopicPartition topicPartition;
    
    /**
     * 主要用于 ACK
     */
    private final OffsetManageableKafkaConsumer<K, V> kafkaConsumer;
    
    private final Predicate<ConsumerRecord<K, V>> handleFunc;
    
    private final Thread thread;
    
    private final int partitionQueueSize;
    
    private final Double inOutRatio;
    
    private boolean isRunning;
    
    public PartitionConsumeWorker(TopicPartition topicPartition, OffsetManageableKafkaConsumer<K, V> kafkaConsumer,
            Predicate<ConsumerRecord<K, V>> func) {
        this(topicPartition, kafkaConsumer, func, 5000, 0.75);
    }
    
    public PartitionConsumeWorker(TopicPartition topicPartition, OffsetManageableKafkaConsumer<K, V> kafkaConsumer,
            Predicate<ConsumerRecord<K, V>> func, int partitionQueueSize, Double inOutRatio) {
        this.topicPartition = topicPartition;
        this.inOutRatio = inOutRatio;
        this.partitionQueueSize = partitionQueueSize;
        this.kafkaConsumer = kafkaConsumer;
        this.partitionQueue = new LinkedBlockingQueue<>(partitionQueueSize);
        this.isRunning = true;
        this.handleFunc = func;
        this.thread = new Thread(this::run);
        this.thread.setName("kafka-partition-worker-for-" + topicPartition.partition());
        this.thread.start();
    }
    
    /**
     * 消费消息
     * @param consumerRecord
     * @param func
     */
    private void consume(ConsumerRecord<K, V> consumerRecord, Predicate<ConsumerRecord<K, V>> func) {
        if (func.test(consumerRecord)) {
            LocalOffsetStorage.getDB().putPartitionLastOffset(topicPartition.partition(), consumerRecord.offset());
            PartitionOffset partitionOffset = new PartitionOffset(topicPartition.partition(), consumerRecord.offset());
            System.out.println("提交了一次位移：" + consumerRecord.partition() + "===" + consumerRecord.offset());
            kafkaConsumer.ack(partitionOffset);
        }
    }
    
    public void offer(ConsumerRecord<K, V> consumerRecord) throws InterruptedException {
        partitionQueue.put(consumerRecord);
    }
    
    private void run() {
        try {
            TimeUnit.MILLISECONDS.sleep(500);
            while (isRunning) {
//                if (partitionQueue.size() / partitionQueueSize <= inOutRatio) {
//                    kafkaConsumer.resume(topicPartition);
//                }
                ConsumerRecord<K, V> consumerRecord = partitionQueue.poll();
                if (consumerRecord != null) {
                    consume(consumerRecord, handleFunc);
                } else {
                    // 防止长期没有消息导致的CPU空转.
                    TimeUnit.MILLISECONDS.sleep(10);
                }
            }
        } catch (InterruptedException e) {
        
        }
        
    }
    
    @Override
    public void close() throws IOException {
        isRunning = false;
    }
    
}
