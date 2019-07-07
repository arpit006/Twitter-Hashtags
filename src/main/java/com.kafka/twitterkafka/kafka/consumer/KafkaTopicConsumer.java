package com.kafka.twitterkafka.kafka.consumer;

import com.kafka.twitterkafka.elasticsearch.ElasticSearchRepository;
import com.kafka.twitterkafka.kafka.config.ConsumerConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Service
public class KafkaTopicConsumer implements IKafkaTopicConsumer {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicConsumer.class);

    private Consumer<String, String> consumer;

    @Autowired
    private ConsumerConfiguration consumerConfiguration;

    @Autowired
    private ElasticSearchRepository repository;

    private String consumerId = "twitter_kafka_4";

    KafkaTopicConsumer() {
        consumer = create();
    }

    public Consumer<String, String> create() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024 * 500);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 100 * 1024 * 1024);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    @Override
    public void execute() {
        consumer.subscribe(Collections.singletonList(consumerConfiguration.getTopic()));
        while (true) {
            try {
                Set<TopicPartition> topicPartitions = consumer.assignment();
                for (TopicPartition partition : topicPartitions) {
                    OffsetAndMetadata committed = consumer.committed(partition);
                    if (committed == null) {
                        consumer.seekToBeginning(Collections.singleton(partition));
                    } else {
                        consumer.seek(partition, committed.offset());
                    }
                }
//                LOGGER.info("Consumer Instance " + Thread.currentThread().getName()+ " Up and running....");
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                consumerRecords.forEach(t -> {
                    LOGGER.info("#### Consuming from topic : " + t.topic() +
                            "\nPartition : " + t.partition() +
                            "\nOffset : " + t.offset() +
                            "\nKey : " + t.key() +
                            "\nValue : " + t.value() +
                            "\nHeaders : " + t.headers() +
                            "\nTHREAD : " + Thread.currentThread().getName());
                    repository.save(t.value());
                });

                consumer.commitSync();

            } catch (Exception e) {
                LOGGER.error(e.toString());
            }
        }
    }
}
