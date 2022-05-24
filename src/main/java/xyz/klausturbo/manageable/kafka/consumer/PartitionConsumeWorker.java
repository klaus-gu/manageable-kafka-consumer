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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Worker for single partition .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class PartitionConsumeWorker<K, V> implements Closeable {
    
    private final BlockingQueue<ConsumerRecord<K, V>> partitionQueue;
    
    private final TopicPartition topicPartition;
    
    /**
     * used for ack.
     */
    private final TurboKafkaConsumer<K, V> kafkaConsumer;
    
    private final Predicate<ConsumerRecord<K, V>> handleFunc;
    
    private final Thread thread;
    
    private final int partitionQueueSize;
    
    private boolean isRunning;
    
    /**
     * Record if current worker has been paused.
     */
    private AtomicBoolean isPaused = new AtomicBoolean(false);
    
    public PartitionConsumeWorker(TopicPartition topicPartition, TurboKafkaConsumer<K, V> kafkaConsumer,
            Predicate<ConsumerRecord<K, V>> func, int partitionQueueSize) {
        this.topicPartition = topicPartition;
        this.partitionQueueSize = partitionQueueSize;
        this.kafkaConsumer = kafkaConsumer;
        this.partitionQueue = new LinkedBlockingQueue<>(partitionQueueSize);
        this.isRunning = true;
        this.handleFunc = func;
        this.thread = new Thread(this::run);
        this.thread.setName("PartitionWorker-" + topicPartition.partition());
        this.thread.start();
        LogUtil.PARTITION_WORKER
                .info("Start a worker[{}] for partition[{}].", thread.getName(), topicPartition.partition());
    }
    
    /**
     * consume message.
     * @param consumerRecord
     * @param func biz code.
     */
    private void consume(ConsumerRecord<K, V> consumerRecord, Predicate<ConsumerRecord<K, V>> func) {
        String key = consumerRecord.topic() + consumerRecord.partition();
        if (!LocalOffsetStorage.getDB().isConsumedBefore(key, consumerRecord.offset()) && func.test(consumerRecord)) {
            // record offset consumed this time,in case that message be consumed twice.
            LocalOffsetStorage.getDB().putPartitionLastOffset(key, consumerRecord.offset());
        }
        PartitionOffset partitionOffset = new PartitionOffset(topicPartition.partition(), consumerRecord.offset());
        kafkaConsumer.ack(partitionOffset);
    }
    
    public boolean offer(ConsumerRecord<K, V> consumerRecord) {
        return partitionQueue.offer(consumerRecord);
    }
    
    private void run() {
        try {
            TimeUnit.MILLISECONDS.sleep(500);
            while (isRunning) {
                ConsumerRecord<K, V> consumerRecord = partitionQueue.poll();
                if (consumerRecord != null) {
                    consume(consumerRecord, handleFunc);
                } else {
                    // avoid cpu idling.
                    TimeUnit.MILLISECONDS.sleep(10);
                    if (isPaused.get()) {
                        setResumed();
                        kafkaConsumer.requestResume(topicPartition.partition());
                    }
                }
            }
        } catch (InterruptedException e) {
            LogUtil.TURBO_KAFKA_CONSUMER.error("[run()]:{}.", e.getMessage());
        }
    }
    
    public void setPaused() {
        this.isPaused.compareAndSet(false, true);
    }
    
    public void setResumed() {
        this.isPaused.compareAndSet(true, false);
    }
    
    public boolean isPaused() {
        return isPaused.get();
    }
    
    @Override
    public void close() throws IOException {
        isRunning = false;
    }
}
