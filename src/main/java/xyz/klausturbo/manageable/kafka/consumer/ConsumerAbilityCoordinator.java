package xyz.klausturbo.manageable.kafka.consumer;

import java.util.concurrent.TimeUnit;

/**
 * The coordinator is used to adjust various abilities .
 * <p></p>
 * ${@link OffsetManageableKafkaConsumer#run()}
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program dc-log-collect
 **/
public class ConsumerAbilityCoordinator {
    
    private static final long STEP = 100L;
    
    private static final long INIT_WAIT_MS = 50L;
    
    private volatile Long init_wait_millies = 50L;
    
    public void intervene() {
        System.out.println("协调者介入。。。介入时间：" + init_wait_millies);
        try {
            TimeUnit.MILLISECONDS.sleep(this.init_wait_millies);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public synchronized void highLoad() {
        this.init_wait_millies += STEP;
    }
    
    public synchronized void free() {
        this.init_wait_millies = INIT_WAIT_MS;
    }
}
