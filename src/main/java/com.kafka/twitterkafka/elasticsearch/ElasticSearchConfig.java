package com.kafka.twitterkafka.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Configuration
public class ElasticSearchConfig {

    @Value("${elasticsearch.server}")
    private String elasticsearchServer;

    @Value("${elasticsearch.port}")
    private int port;

    @Value("${elasticsearch.port-java}")
    private int javaPort;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(elasticsearchServer, port, "http"),
                        new HttpHost(elasticsearchServer, javaPort, "http")
                ));
    }
}
