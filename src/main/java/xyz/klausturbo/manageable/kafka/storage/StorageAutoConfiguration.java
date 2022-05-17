package xyz.klausturbo.manageable.kafka.storage;

import org.rocksdb.RocksDBException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 存储配置 .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
@Configuration
public class StorageAutoConfiguration {
    
    @Bean
    public LocalOffsetStorage localOffsetStorage() throws RocksDBException {
        return new LocalOffsetStorage();
    }
}
