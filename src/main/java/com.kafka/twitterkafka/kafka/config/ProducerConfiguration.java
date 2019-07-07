package com.kafka.twitterkafka.kafka.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Configuration
@Data
public class ProducerConfiguration {

    @Value("${kafka.bootstrap-server}")
    private String BOOTSTRAP_SERVER;

    @Value("${kafka.keySerializer}")
    private String KEY_SERIALIZER;

    @Value("${kafka.valueSerializer}")
    private String VALUE_SERIALIZER;

    @Value("${kafka.acksConfig}")
    private String ACKS_CONFIG;

    @Value("${kafka.retryBack}")
    private String RETRY_BACK_TIME;

    @Value("${kafka.retries}")
    private String RETRIES;

    @Value("${kafka.clientId}")
    private String CLIENT_ID;

    @Value("${kafka.enableIdempotence}")
    private String ENABLE_IDEMPOTENCE;

    @Value("${kafka.maxRequestPerConnection}")
    private String MAX_REQUEST_PER_CONNECTION;

    @Value("${kafka.topic}")
    private String topic;
}
