package com.moonz.study.consumer;

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
 * 특정 단위 시간마다 자동으로(auto) 커밋을 수행하는 컨슈머.
 */
public class ConsumerWithAutoCommit {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithAutoCommit.class);
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

        // optional config
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);    // default
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 60000);  // 60s

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            logger.info("records: {}", records);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record : {}", record);
            }
        }

    }
}
