package com.moonz.study.consumer;

import com.moonz.study.rebalancelistener.RebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * 구현한 RebalanceListener 를 전달하여, 리밸런싱이 발생할 때 전/후에 호출되도록 하였다.
 */
public class ConsumerWithRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithRebalanceListener.class);
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";
    private static final String GROUP_ID = "test-group";

    /**
     * 브로커에 데이터가 저장되면 이를 가져와서 로깅하는 로직 작성.
     */
    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // 구독을 할 때 리밸런스 리스너를 전달한다.
        consumer.subscribe(List.of(TOPIC_NAME), new RebalanceListener());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            logger.info("records: {}", records);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record : {}", record);
            }
        }

    }
}
