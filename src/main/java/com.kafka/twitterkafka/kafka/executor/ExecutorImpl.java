package com.kafka.twitterkafka.kafka.executor;

import com.kafka.twitterkafka.kafka.config.ConsumerConfiguration;
import com.kafka.twitterkafka.kafka.consumer.IKafkaTopicConsumer;
import com.kafka.twitterkafka.kafka.job.ConsumerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Component
public class ExecutorImpl implements IExecutor {

    private final Logger LOGGER = LoggerFactory.getLogger(ExecutorImpl.class);

    private ExecutorService executorService;

    @Autowired
    private ConsumerConfiguration consumerConfiguration;

    @Autowired
    private IKafkaTopicConsumer iKafkaTopicConsumer;

    @Override
    public void execute() {
        int partitions = consumerConfiguration.getPartitions();
        executorService = Executors.newFixedThreadPool(partitions);
        IntStream
                .range(0, partitions)
                .parallel()
                .forEach(t -> {
                    executorService.submit(new ConsumerJob(iKafkaTopicConsumer));
                    LOGGER.info("Consumer Instance :: => " + (t + 1) + " is up and running........"+
                            "\nThread for Instance : -> "+ Thread.currentThread().getName());
                });
    }
}
