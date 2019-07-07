package com.kafka.twitterkafka.kafka.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
public class ZookeeperConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperConfigurer.class);
    private static AdminClient adminClient;

    static {
        initialize();
    }

    private static void initialize() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        adminClient = AdminClient.create(properties);
    }


    public static boolean checkTopic(String inTopicName) {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topicNamesList = null;
        try {
            topicNamesList = listTopicsResult.names().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        if (topicNamesList == null) {
            LOGGER.info("No topics present in Kafka");
        } else {
            if (topicNamesList.contains(inTopicName)) {
                return true;
            }
        }
        LOGGER.info("Topic " + inTopicName + " is not present in Kafka");
        return false;
    }

    public static boolean checkConsumerGroups(String inConsumerId) {
        ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = null;
        Set<String> groupIds = new HashSet<>();
        try {
            consumerGroupListings = listConsumerGroupsResult.all().get();
            groupIds.addAll(consumerGroupListings
                    .stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toSet()));

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        if (groupIds.contains(inConsumerId)) {
            return true;
        } else {
            LOGGER.info("Consumer Group with Group ID : " + inConsumerId + " not found.");
            return false;
        }
    }

    public static void createTopic(String inTopicName, int inPartition) {
        deleteTopic(inTopicName);
        NewTopic newTopic = new NewTopic(inTopicName, inPartition, (short) 1);
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
        LOGGER.info("Topic " + inTopicName + " created with No of Partitions = " + inPartition +
                " and a Replication Factor of " + 1);
        LOGGER.info("Create Topic : " + topics.values().toString());
    }

    public static void deleteTopic(String inTopicName) {
        boolean isTopicPresent = checkTopic(inTopicName);
        if (isTopicPresent) {
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(inTopicName));
            LOGGER.info("Topic " + inTopicName + " ");
            LOGGER.info(deleteTopicsResult.values().toString());
        } else {
            LOGGER.info("Topic " + inTopicName + " cannot be deleted.");
        }
    }

    public static void increasePartitionOfTopic(String inTopicName, int inPartition) {
        boolean isTopicPresent = checkTopic(inTopicName);
        if (isTopicPresent) {
            NewPartitions newPartitions = NewPartitions.increaseTo(inPartition);
            Map<String, NewPartitions> partitionsMap = new HashMap<>();
            partitionsMap.put(inTopicName, newPartitions);
            CreatePartitionsResult partitions = adminClient.createPartitions(partitionsMap);
            LOGGER.info("Partition of Topic " + inTopicName + " increased to " + inPartition);
            LOGGER.info(partitions.values().toString());
        } else {
            LOGGER.info("Partitions for the Topic " + inTopicName + " cannot be increased");
        }
    }

    public static void deleteConsumerGroups(String consumerId) {
        DeleteConsumerGroupsResult deleteConsumerGroupsResult =
                adminClient.deleteConsumerGroups(Collections.singletonList(consumerId));

        LOGGER.info(deleteConsumerGroupsResult.deletedGroups().toString());
    }
}
