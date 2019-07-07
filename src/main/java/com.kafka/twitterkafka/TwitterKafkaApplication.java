package com.kafka.twitterkafka;

import com.kafka.twitterkafka.kafka.executor.ExecutorImpl;
import com.kafka.twitterkafka.kafka.executor.IExecutor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class TwitterKafkaApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(TwitterKafkaApplication.class, args);
        IExecutor executor = applicationContext.getBean(ExecutorImpl.class);
        executor.execute();
    }

}
