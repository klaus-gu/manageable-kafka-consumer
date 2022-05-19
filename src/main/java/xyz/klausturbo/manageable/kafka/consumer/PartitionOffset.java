package xyz.klausturbo.manageable.kafka.consumer;

/**
 * Partition Offset relation .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class PartitionOffset {
    
    /**
     * ${@link org.apache.kafka.common.TopicPartition#partition()}
     */
    private final int partition;
    
    private final long offset;
    
    public PartitionOffset(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }
    
    public int partition() {
        return partition;
    }
    
    public long offset() {
        return offset;
    }
}
