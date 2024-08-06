package com.moonz.study.rebalancelistener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.warn("리밸런싱이 발생하기 직전에 파티션들이 어떻게 할당되어있는지 상태 확인 : {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.warn("리밸런스가 끝나서 파티션들이 할당(assigned)되었다. : {}", partitions);
    }
}
