package com.kafka.twitterkafka.kafka.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Configuration
@Data
public class ConsumerConfiguration {

    @Value("${kafka.topic.partitions}")
    private int partitions;

    @Value("${kafka.topic}")
    private String topic;
}
