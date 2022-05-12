package xyz.klausturbo.manageable.kafka.consumer;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.AbstractMatcherFilter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * 消费者日志过滤器 .
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class KafkaConsumerLogFilter extends AbstractMatcherFilter<ILoggingEvent> {
    
    @Override
    public FilterReply decide(ILoggingEvent iLoggingEvent) {
        return null;
    }
    
}
