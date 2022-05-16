package xyz.klausturbo.manageable.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

/**
 * Save offset at local .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class LocalOffsetPersistService {
    
    private final Map<Integer, Long> localOffsetMap = new HashMap<>();
    
    public Long getLastPartitionOffset(Integer partitionId) {
        return localOffsetMap.get(partitionId);
    }
    
    public void setCurrentPartitionOffset(Integer partitionId, Long offset) {
        localOffsetMap.put(partitionId, offset);
    }
}
