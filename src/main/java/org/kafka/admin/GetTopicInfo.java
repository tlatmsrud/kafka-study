package org.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * title        :
 * author       : sim
 * date         : 2023-11-14
 * description  :
 */
public class GetTopicInfo {
    private static final Logger logger = LoggerFactory.getLogger(GetTopicInfo.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "211.46.18.190:9092");

        try(AdminClient admin = AdminClient.create(configs)){
            Map<String, TopicDescription> topicInfo = admin.describeTopics(Collections.singletonList("test3")).all().get();
            logger.info("{}", topicInfo);
        }
    }
}
