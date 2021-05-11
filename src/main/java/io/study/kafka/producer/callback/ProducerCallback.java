package io.study.kafka.producer.callback;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class ProducerCallback implements Callback {
    
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null) {
            log.error(e.getMessage(), e);
        }
        else {
            log.info("message '{}' send done !", recordMetadata.toString());
        }
    }
    
}
