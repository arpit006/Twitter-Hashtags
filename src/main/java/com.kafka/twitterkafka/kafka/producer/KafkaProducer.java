package com.kafka.twitterkafka.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Component
public class KafkaProducer {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void postToKafkaTopic(String inJson, String inTopic) {
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(inTopic, inJson);

        send.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("Could not save this data to Kafka Topic : " + inTopic +
                        "\nERROR :-> " + ex);
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("####Posting to Kafka topic :::: "+
                        "\nTOPIC :-> "+result.getRecordMetadata().topic()+
                        "\nPARTITION :-> "+result.getRecordMetadata().partition()+
                        "\nOFFSET : -> "+result.getRecordMetadata().offset() +
                        "\nRECORD :-> "+result.getProducerRecord().toString());
            }
        });
    }
}
