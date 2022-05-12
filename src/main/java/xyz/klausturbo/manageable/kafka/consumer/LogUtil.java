package xyz.klausturbo.manageable.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志统一输出 .
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class LogUtil {
    public static final Logger KAFKA_CONSUMER_LOGGER;
    
    static {
        KAFKA_CONSUMER_LOGGER = LoggerFactory.getLogger("manageable.kafka.consumer.running-log");
    }
    
    public static Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }
}
