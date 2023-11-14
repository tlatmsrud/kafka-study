package org.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * title        :
 * author       : sim
 * date         : 2023-11-14
 * description  :
 */
public class GetBrokerInfo {

    private static final Logger logger = LoggerFactory.getLogger(GetBrokerInfo.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "211.46.18.190:9092");

        AdminClient admin = AdminClient.create(configs);

        System.out.println("== Get broker information");

        for(Node node : admin.describeCluster().nodes().get()){
            logger.info("node : {}", node);
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigsResult = admin.describeConfigs(Collections.singleton(cr));

            describeConfigsResult.all().get().forEach((broker, config) ->
                    config.entries().forEach(configEntry -> logger.info(configEntry.name() + "= " + configEntry.value())));
        }
    }
}
