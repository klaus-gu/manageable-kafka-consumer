package xyz.klausturbo.manageable.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Worker for single partition .
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class PartitionConsumeWorker<K, V> implements Runnable {
    
    /**
     * The queued records before delivering to the biz, are kept here.
     */
    private final BlockingQueue<ConsumerRecord<K, V>> queuedRecords;
    
    /**
     * 分区消费者关注的分区信息
     */
    private final Integer partitionId;
    
    private final int maxQueuedRecords;
    
    public PartitionConsumeWorker(Integer partitionId) {
        this(partitionId, 10_000);
    }
    
    public PartitionConsumeWorker(Integer partitionId, int maxQueuedRecords) {
        this.partitionId = partitionId;
        this.maxQueuedRecords = maxQueuedRecords;
        this.queuedRecords = new ArrayBlockingQueue<>(maxQueuedRecords);
        
    }
    
    public void queue(ConsumerRecord<K, V> record) {
        System.err.println("分区：" + partitionId + " >>> " + record.key() + " ==>> " + record.value());
        if (queuedRecords.offer(record)) {
        
        }
    }
    
    @Override
    public void run() {
    
    }
}
