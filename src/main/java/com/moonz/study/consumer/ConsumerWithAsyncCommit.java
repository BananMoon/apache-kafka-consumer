package com.moonz.study.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * 레코드를 처리 후 수동으로 커밋하는데,
 * 커밋 작업을 기다리지 않고, 스레드가 비동기적으로 동작되도록 설정한다. -> 비동기 커밋 수행.
 */
public class ConsumerWithAsyncCommit {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithAsyncCommit.class);
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";
    private static final String GROUP_ID = "test-group";

    /**
     * 브로커에 데이터가 저장되면 이를 가져와서 로깅하는 로직 작성.
     * 데이터 처리에 직접적인 영향은 없지만, 리밸런싱이 일어나거나 데이터가 중단되거나, 컨슈머가 중단되었을 때 어떻게 처리할지 고민해보면 좋다.
     * 어느 상황에서 수동 동기 커밋할지 수동 비동기 커밋할지, 혹은 자동 커밋할지를 정하는 것이 중요하다.
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
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (Objects.isNull(e)) {
                        logger.info("Commit succeeded");
                    } else {
                        logger.error("Commit failed for offsets {} (exception : {})", offsets, e);
                    }
                }
            });
        }

    }
}
