package xyz.klausturbo.manageable.kafka.consumer;

import org.junit.Assert;
import org.junit.Test;

import java.util.OptionalLong;

/**
 * ${@link OffsetMonitor} .
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class OffsetMonitor_TEST {
    
    @Test
    public void margin_Page_TEST() {
        OffsetMonitor offsetMonitor = new OffsetMonitor(1, 2);
        // open first page from 1 - 2.
        offsetMonitor.track(1, 1);
        boolean trackSuccess = offsetMonitor.track(1, 2);
        Assert.assertTrue(trackSuccess);
        
        // return false when reach monitor pagesize.
        boolean trackFail = offsetMonitor.track(1, 3);
        Assert.assertFalse(trackFail);
        
        OffsetMonitor offsetMonitor2 = new OffsetMonitor(5,2);
        // open first page from  2 - 3
        offsetMonitor2.track(1, 2);
        offsetMonitor2.track(1,3);
        // open second page from 4 - 5
        offsetMonitor2.track(1,4);
        offsetMonitor2.track(1,5);
        
        OptionalLong offsetToCommit = offsetMonitor2.ack(1,2);
        OptionalLong offsetToCommit2 = offsetMonitor2.ack(1,3);
        System.out.println(offsetToCommit);
        System.out.println(offsetToCommit2.getAsLong());
    }
    
}
