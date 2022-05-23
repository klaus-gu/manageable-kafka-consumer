package xyz.klausturbo.manageable.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志统一输出 .
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class LogUtil {
    
    public static final Logger TURBO_KAFKA_CONSUMER;
    public static final Logger PARTITION_WORKER;
    
    static {
        TURBO_KAFKA_CONSUMER = LoggerFactory.getLogger("turboKafkaConsumer");
        PARTITION_WORKER=LoggerFactory.getLogger("partitionWorker");
    }
    
    public static Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }
}
