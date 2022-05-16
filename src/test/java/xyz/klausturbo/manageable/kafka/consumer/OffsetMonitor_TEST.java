package xyz.klausturbo.manageable.kafka.consumer;

import org.junit.Assert;
import org.junit.Test;

import java.util.OptionalLong;

/**
 * ${@link OffsetMonitor} .
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class OffsetMonitor_TEST {
    
    @Test
    public void disordered_Ack_Offset_TEST() {
        final OffsetMonitor offsetMonitor = new OffsetMonitor(4, 2);
        offsetMonitor.track(1, 0);
        offsetMonitor.track(1, 1);
        offsetMonitor.track(1, 2);
        offsetMonitor.track(1, 3);
        
        offsetMonitor.ack(1, 2);
        offsetMonitor.ack(1, 3);
        offsetMonitor.ack(1, 1);
        OptionalLong optionalLong = offsetMonitor.ack(1, 0);
        Assert.assertTrue(optionalLong.isPresent());
        Assert.assertEquals(4, optionalLong.getAsLong());
        
    }
    
    @Test
    public void margin_Page_TEST() {
        OffsetMonitor offsetMonitor = new OffsetMonitor(2, 2);
        // open first page from 1 - 2.
        offsetMonitor.track(1, 1);
        boolean trackSuccess = offsetMonitor.track(1, 2);
        Assert.assertTrue(trackSuccess);
        
        // open second page from 3 - 4
        trackSuccess = offsetMonitor.track(1, 3);
        Assert.assertTrue(trackSuccess);
        offsetMonitor.track(1, 4);
        // return false when reach monitor pagesize.
        boolean trackFail = offsetMonitor.track(1, 5);
        Assert.assertFalse(trackFail);
        
        OffsetMonitor offsetMonitor2 = new OffsetMonitor(2, 2);
        // open first page from  2 - 3
        offsetMonitor2.track(1, 2);
        offsetMonitor2.track(1, 3);
        // open second page from 4 - 5
        offsetMonitor2.track(1, 4);
        offsetMonitor2.track(1, 5);
        
        OptionalLong offsetToCommit = offsetMonitor2.ack(1, 2);
        OptionalLong offsetToCommit2 = offsetMonitor2.ack(1, 3);
        System.out.println(offsetToCommit);
        System.out.println(offsetToCommit2.getAsLong());
    }
    
}
