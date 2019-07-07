package com.kafka.twitterkafka.hosebird.service;

import com.google.common.collect.Lists;
import com.kafka.twitterkafka.hosebird.config.TwitterConfiguration;
import com.kafka.twitterkafka.kafka.config.ProducerConfiguration;
import com.kafka.twitterkafka.kafka.service.KafkaService;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Service
public class TwitterService {

    private Logger LOGGER = LoggerFactory.getLogger(TwitterService.class);

    private Authentication hoseBirdAuth;

    @Autowired
    private TwitterConfiguration twitterConfiguration;

    @Autowired
    private ProducerConfiguration producerConfiguration;

    @Autowired
    private KafkaService kafkaService;

    @PostConstruct
    private void initialize() {
        hoseBirdAuth = new OAuth1(twitterConfiguration.getApiKey(),
                twitterConfiguration.getApiSecret(),
                twitterConfiguration.getToken(),
                twitterConfiguration.getTokenSecret());
    }


    public void queryThisHashTag(String inHashTag) {

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndPoint = new StatusesFilterEndpoint();
        List<String> hashtags = Lists.newArrayList(inHashTag);
        hoseBirdEndPoint.trackTerms(hashtags);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hoseBirdHosts)
                .authentication(hoseBirdAuth)
                .endpoint(hoseBirdEndPoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hoseBirdClient = builder.build();

        hoseBirdClient.connect();

        while (!hoseBirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hoseBirdClient.stop();
            }
            if (msg != null) {
                LOGGER.info(msg);
                kafkaService.postToKafkaTopic(msg, producerConfiguration.getTopic());
            }
        }
        LOGGER.info("END of Streaming Twiiter api.....");
    }
}
