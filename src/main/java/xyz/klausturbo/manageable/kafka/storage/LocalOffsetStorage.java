package xyz.klausturbo.manageable.kafka.storage;

import lombok.val;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.SizeUnit;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.StringUtils;

/**
 * 用于存储成功消费的 offset .
 * <p>
 * 考虑到 offset消息 的消费与最终的 offset 的提交之间存在系统崩溃的可能，采用每次消费成功之后，本地持久化本次消费的消息的offset，
 * <p>
 * 用于系统恢复之后offset的再提交以及防止消息的重复消费.
 * <p>
 * 例如：消费者单次拉 500 条消息，并且手动提交设置为 500，当消费成功 200 条之后，系统出现崩溃，此时的前200 条消息已经成功消费.
 * <p>
 * 但offset还未提交到kafka,当再次恢复之后，前200条消息可能重复消费（排除业务逻辑的幂等）.
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class LocalOffsetStorage implements DisposableBean, ApplicationContextAware {
    
    private static final String DATA_PATH = "/db";
    
    private static ApplicationContext applicationContext;
    
    static {
        RocksDB.loadLibrary();
    }
    
    private final RocksDB rocksDB;
    
    private final Options options;
    
    public LocalOffsetStorage() throws RocksDBException {
        String dataPath = System.getProperty("user.dir") + DATA_PATH;
        this.options = new Options();
        options.setCreateIfMissing(true).setWriteBufferSize(8 * SizeUnit.KB).setMaxWriteBufferNumber(3)
                .setMaxBackgroundJobs(10).setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL);
        this.rocksDB = RocksDB.open(options, dataPath);
    }
    
    public static LocalOffsetStorage getDB() {
        return applicationContext.getBeanProvider(LocalOffsetStorage.class).getIfAvailable();
    }
    
    public Long getPartitionLastOffset(Integer partitionId) {
        String res = null;
        try {
            final byte[] bytes = rocksDB.get(partitionId.toString().getBytes());
            if (bytes == null){
                return null;
            }
            res = new String(bytes);
        } catch (RocksDBException e) {
            // todo 日志
            e.printStackTrace();
        }
        if (StringUtils.hasLength(res)) {
            return Long.parseLong(res);
        }
        return null;
    }
    
    public void putPartitionLastOffset(Integer partitionId, Long offset) {
        try {
            rocksDB.put(partitionId.toString().getBytes(), offset.toString().getBytes());
        } catch (RocksDBException e) {
            // todo 日志
            e.printStackTrace();
        }
    }
    
    public boolean isConsumedBefore(Integer partitionId, Long currentOffset) {
        Long lastOffset = getPartitionLastOffset(partitionId);
        if (null == lastOffset){
            return false;
        }
        return currentOffset <= lastOffset;
    }
    
    @Override
    public void destroy() throws Exception {
        this.rocksDB.close();
        this.options.close();
    }
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        LocalOffsetStorage.applicationContext = applicationContext;
    }
}
