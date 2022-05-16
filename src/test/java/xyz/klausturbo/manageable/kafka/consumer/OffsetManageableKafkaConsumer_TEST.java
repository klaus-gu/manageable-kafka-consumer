package xyz.klausturbo.manageable.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ${@link OffsetManageableKafkaConsumer} .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class OffsetManageableKafkaConsumer_TEST {
    
    private static final String BROKERS = "127.0.0.1:9092";
    
    private static final String GROUP_ID = "test-group-02";
    
    private static final String TOPIC = "smart-commit-topic";
    
    @Test
    public void consume_TEST() throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        OffsetManageableKafkaConsumer<String, String> consumer = new OffsetManageableKafkaConsumer<String, String>(
                properties, 10, 1000, 5000);
        consumer.subscribe(TOPIC);
        consumer.start();
        
        Map<Integer, Long> partitionOffsetMap = new HashMap<>();
        
        List<PartitionOffset> partitionOffsets = new ArrayList<>(763);
        
        while (partitionOffsets.size() < 763) {
            ConsumerRecord<String, String> record = consumer.poll();
            if (record != null) {
                partitionOffsetMap.put(record.partition(), record.offset());
                partitionOffsets.add(new PartitionOffset(record.partition(), record.offset()));
                System.err.println("消费消息：" + record.value());
            }
        }
        
        for (PartitionOffset offset : partitionOffsets) {
            consumer.ack(offset);
        }
    }
}
