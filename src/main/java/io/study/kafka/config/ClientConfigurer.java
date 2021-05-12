package io.study.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ClientConfigurer {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        try(AdminClient admin = AdminClient.create(properties)) {
            // AdminClient information
            for(Node node : admin.describeCluster().nodes().get()) {
                log.info("node : {}", node);
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
                
                Map<ConfigResource, Config> configResourceConfigMap = admin.describeConfigs(Collections.singleton(configResource)).all().get();
                configResourceConfigMap.forEach((broker, config)->{
                    config.entries().forEach(configEntry->log.info(configEntry.name() + " = " + configEntry.value()));
                });
            }
            
            Map<String, TopicDescription> topicDescriptionMap = admin.describeTopics(Collections.singletonList("topic_test_2")).all().get();
            log.info("topic information = {}", topicDescriptionMap);
        }
        
    }
    
}
