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
 * 수동으로 오프셋 커밋하는 컨슈머
 * 레코드 처리가 끝나고 응답받은 후에 동기적으로 커밋을 수행한다.
 * 그래야 다음 레코드를 처리하거나 리밸런싱이 발생했을 때 안전하게 데이터를 처리할 수 있다.
 * 레코드 처리가 끝난 후에 동기적으로 커밋하기 때문에 비동기 커밋보다는 상대적으로 시간 지연이 발생한다.
 */
public class ConsumerWithSyncCommit {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithSyncCommit.class);
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";
    private static final String GROUP_ID = "test-group";

    /**
     * 브로커에 데이터가 저장되면 이를 가져와서 로깅하는 로직.
     */
    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // optional config
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            logger.info("records: {}", records);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record : {}", record);
            }
            consumer.commitSync();  // 수동 동기 커밋
        }
    }

}
