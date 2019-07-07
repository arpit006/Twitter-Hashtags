package com.kafka.twitterkafka.kafka.job;

import com.kafka.twitterkafka.kafka.consumer.IKafkaTopicConsumer;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class ConsumerJob implements Runnable {


    private IKafkaTopicConsumer iKafkaTopicConsumer;

    public ConsumerJob(IKafkaTopicConsumer iKafkaTopicConsumer) {
        this.iKafkaTopicConsumer = iKafkaTopicConsumer;
    }

    @Override
    public void run() {
        iKafkaTopicConsumer.execute();
    }
}
