package org.kafka.consumer.asyncCommit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * title        :
 * author       : sim
 * date         : 2023-11-14
 * description  :
 */
public class KafkaConsumerRebalanceListener {

    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test3";
    private final static String BOOTSTRAP_SERVERS = "211.46.18.190:9092";
    private final static String GROUP_ID = "test-group";
    private final static int PARTITION_NUMBER = 0;

    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    public static void main(String[] args){

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 리밸런스 발생 시 수동커밋을 위함

        consumer = new KafkaConsumer<>(configs);
        //consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
        Set<TopicPartition> assignedTopicPartition = consumer.assignment();

        Iterator<TopicPartition> list = assignedTopicPartition.iterator();

        while(list.hasNext()){
            TopicPartition partition = list.next();
            System.out.println(partition.topic());
            System.out.println(partition.partition());
        }

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for(ConsumerRecord<String, String> record : records){
                logger.info("{}", record);
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null));

                consumer.commitSync();
            }

        }
    }

    public static class RebalanceListener implements ConsumerRebalanceListener{

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned");
        }
    }
}
