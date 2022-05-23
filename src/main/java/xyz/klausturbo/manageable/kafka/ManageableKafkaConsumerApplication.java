package xyz.klausturbo.manageable.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import xyz.klausturbo.manageable.kafka.consumer.TurboKafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 启动类 .
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
@SpringBootApplication
public class ManageableKafkaConsumerApplication {
    private static final String BROKERS = "47.98.217.98:19091,47.98.167.121:19091";
    
    private static final String GROUP_ID = "test-group-02";
    
    private static final String TOPIC = "smart-commit-topic";
    
    public static void main(String[] args) throws IOException, InterruptedException {
        SpringApplication.run(ManageableKafkaConsumerApplication.class, args);
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"5000");
        TurboKafkaConsumer<String,String> runner = new TurboKafkaConsumer<>(properties, 10,10,5000);
        List<Integer> partitions = Arrays.asList(new Integer[] {0, 1, 2, 3, 4, 5, 6, 7});
        runner.assign(TOPIC,partitions);
        runner.start();
    }
    
}
