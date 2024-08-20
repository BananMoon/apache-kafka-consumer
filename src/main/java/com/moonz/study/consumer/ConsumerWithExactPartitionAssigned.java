package com.moonz.study.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 특정 토픽의, 특정 파티션을 지정하여 할당받는 컨슈머
 */
public class ConsumerWithExactPartitionAssigned {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithExactPartitionAssigned.class);
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";
    private static final int PARTITION_NUMBER = 0;


    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            logger.info("records: {}", records);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record : {}", record);
            }
        }
    }

}
