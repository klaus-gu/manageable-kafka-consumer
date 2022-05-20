package xyz.klausturbo.manageable.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import xyz.klausturbo.manageable.kafka.storage.LocalOffsetStorage;
import xyz.klausturbo.manageable.kafka.util.LogUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
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
    
    private static final Logger LOGGER = LogUtil.getLogger(PartitionConsumeWorker.class);
    
    private final BlockingQueue<ConsumerRecord<K, V>> partitionQueue;
    
    /**
     * 分区消费者关注的分区信息
     */
    private final TopicPartition topicPartition;
    
    /**
     * 主要用于 ACK
     */
    private final TurboKafkaConsumer<K, V> kafkaConsumer;
    
    private final Predicate<ConsumerRecord<K, V>> handleFunc;
    
    private final Thread thread;
    
    private final int partitionQueueSize;
    
    private boolean isRunning;
    
    /**
     * 当前Worker的分区是否被暂定消费.
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
        this.thread.setName("kafka-partition-worker-for-" + topicPartition.partition());
        this.thread.start();
        LOGGER.info("Start a worker[{}] for partition[{}].", thread.getName(), topicPartition.partition());
    }
    
    /**
     * 消费消息
     * @param consumerRecord
     * @param func
     */
    private void consume(ConsumerRecord<K, V> consumerRecord, Predicate<ConsumerRecord<K, V>> func) {
        if (!LocalOffsetStorage.getDB().isConsumedBefore(consumerRecord.partition(), consumerRecord.offset()) && func.test(consumerRecord)) {
            // 记录本次消息处理的位置，防止在消息被消费并且未提交offset的时候崩溃，在恢复的时候重复消费
            LocalOffsetStorage.getDB().putPartitionLastOffset(topicPartition.partition(), consumerRecord.offset());
        }
        PartitionOffset partitionOffset = new PartitionOffset(topicPartition.partition(), consumerRecord.offset());
        kafkaConsumer.ack(partitionOffset);
    }
    
    public boolean offer(ConsumerRecord<K, V> consumerRecord) {
        return partitionQueue.offer(consumerRecord);
    }
    
    public void offer(List<ConsumerRecord<K, V>> consumerRecords) {
        consumerRecords.forEach(this::offer);
    }
    
    private void run() {
        try {
            TimeUnit.MILLISECONDS.sleep(500);
            while (isRunning) {
                ConsumerRecord<K, V> consumerRecord = partitionQueue.poll();
                if (consumerRecord != null) {
                    consume(consumerRecord, handleFunc);
                } else {
                    // 防止长期没有消息导致的CPU空转.
                    TimeUnit.MILLISECONDS.sleep(10);
                    if (isPaused.get()) {
                        setResumed();
                        kafkaConsumer.requestResume(topicPartition.partition());
                    }
                }
            }
        } catch (InterruptedException e) {
        
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
