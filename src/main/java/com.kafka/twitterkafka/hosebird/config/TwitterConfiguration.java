package com.kafka.twitterkafka.hosebird.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Configuration
@Data
public class TwitterConfiguration {

    @Value("${twitter.apikey}")
    private String apiKey;

    @Value("${twitter.apiSecretKey}")
    private String apiSecret;

    @Value("${twitter.accessToken}")
    private String token;

    @Value("${twitter.accessTokenSecret}")
    private String tokenSecret;
}
