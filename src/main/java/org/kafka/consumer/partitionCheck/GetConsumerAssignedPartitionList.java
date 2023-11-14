package org.kafka.consumer.partitionCheck;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * title        :
 * author       : sim
 * date         : 2023-11-14
 * description  :
 */
public class GetConsumerAssignedPartitionList {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test3";
    private final static String BOOTSTRAP_SERVERS = "211.46.18.190:9092";
    private final static String GROUP_ID = "test-group";
    private final static int PARTITION_NUMBER = 0;

    public static void main(String[] args){

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
        Set<TopicPartition> assignedTopicPartition = consumer.assignment();
        Iterator<TopicPartition> list = assignedTopicPartition.iterator();
        consumer.close();
        while(list.hasNext()){
            TopicPartition partition = list.next();
            System.out.println(partition.topic());
            System.out.println(partition.partition());
        }
    }
}
