package io.study.kafka.simple;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class SimpleProducer {
    
    private final static String TOPIC_NAME = "topic_test_3";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        String key = "key";
        String message = "안녕하세요. 카프카입니다";
        ProducerRecord<String, String> recode = new ProducerRecord<>(TOPIC_NAME, key, message);
        
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            Future<RecordMetadata> send = producer.send(recode, new SimpleProducerCallback());
            
            log.info("transmitted message: {}\n", recode);
            
            producer.flush();
        }
    }
    
}
