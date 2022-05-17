package xyz.klausturbo.manageable.kafka.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import xyz.klausturbo.manageable.kafka.consumer.OffsetManageableKafkaConsumer;
import xyz.klausturbo.manageable.kafka.consumer.PartitionOffset;
import xyz.klausturbo.manageable.kafka.storage.LocalOffsetStorage;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Predicate;

/**
 * Worker for single partition .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class PartitionConsumeWorker<K, V> {
    
    private final BlockingQueue<ConsumerRecord<K,V>> partitionQueue ;
    
    /**
     * 分区消费者关注的分区信息
     */
    private final Integer partitionId;
    
    private final OffsetManageableKafkaConsumer<K, V> kafkaConsumer;
    
    public PartitionConsumeWorker(Integer partitionId, OffsetManageableKafkaConsumer<K, V> kafkaConsumer,int partitionQueueSize) {
        this.partitionId = partitionId;
        this.kafkaConsumer = kafkaConsumer;
        this.partitionQueue = new ArrayBlockingQueue<ConsumerRecord<K,V>>(partitionQueueSize);
    }
    
    /**
     * 消费消息
     * @param consumerRecord
     * @param func
     */
    public void consume(ConsumerRecord<K, V> consumerRecord, Predicate<ConsumerRecord<K, V>> func) {
        if (func.test(consumerRecord)) {
            LocalOffsetStorage.getDB().putPartitionLastOffset(partitionId, consumerRecord.offset());
            PartitionOffset partitionOffset = new PartitionOffset(partitionId, consumerRecord.offset());
            kafkaConsumer.ack(partitionOffset);
        }
    }
}
