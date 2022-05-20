package xyz.klausturbo.manageable.kafka.consumer;

import lombok.Getter;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationEvent;

/**
 * 分区恢复消费请求 .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class PartitionResumeRequest  {
    
    private TopicPartition topicPartition;
    
    public PartitionResumeRequest(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }
    
    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
