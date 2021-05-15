package io.study.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class SubscribeConsumer {
    
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String TOPIC_NAME = "subscribe";
    private final static String GROUP_ID = "subscribe-group";
    private final static int CONSUMER_COUNT = 1;
    private final static List<SubscribeConsumerWorker> workers = new ArrayList<>();
    
    public static void run() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            
            @Override
            public void run() {
                log.info("Shutdown hook");
                workers.forEach(SubscribeConsumerWorker::stopAndWakeup);
            }
        });
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    
        ExecutorService executorService = Executors.newCachedThreadPool();
        for(int i = 0; i < CONSUMER_COUNT; i++) {
            workers.add(new SubscribeConsumerWorker(props, TOPIC_NAME, i));
        }
        workers.forEach(executorService::execute);
    }
    
}
