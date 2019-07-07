package com.kafka.twitterkafka.kafka.service;

import com.kafka.twitterkafka.kafka.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Service
public class KafkaService {

    @Autowired
    private KafkaProducer kafkaProducer;

    public void postToKafkaTopic(String inJson, String inTopic) {
        /*boolean value = ZookeeperConfigurer.checkTopic(inTopic);
        if (!value) {
            ZookeeperConfigurer.createTopic(inTopic, 12);
        } else {
            ZookeeperConfigurer.increasePartitionOfTopic(inTopic, 12);
        }*/
        kafkaProducer.postToKafkaTopic(inJson, inTopic);
    }
}
