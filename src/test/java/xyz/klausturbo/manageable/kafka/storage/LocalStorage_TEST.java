package xyz.klausturbo.manageable.kafka.storage;

import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 * 本地存储测试 .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class LocalStorage_TEST {
    
    @Test
    public void Storage_TEST() throws RocksDBException {
        LocalOffsetStorage storage = new LocalOffsetStorage();
        storage.putPartitionLastOffset(1,9809L);
        Assert.assertEquals(java.util.Optional.of(9809L).get(),storage.getPartitionLastOffset(1));
    }
    
    @Test
    public void getBeforeOffset_TEST() throws RocksDBException {
        LocalOffsetStorage storage = new LocalOffsetStorage();
        System.out.println(storage.getPartitionLastOffset(0));
//        Assert.assertEquals(java.util.Optional.of(9809L).get(),storage.getPartitionLastOffset(0));
    }
}
