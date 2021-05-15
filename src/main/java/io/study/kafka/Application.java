package io.study.kafka;

import io.study.kafka.consumer.SubscribeConsumer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;

@SpringBootApplication
public class Application {
    
    public static void main(String[] args) {
        //SpringApplication.run(Application.class, args);
        SubscribeConsumer.run();
    }
    
    @Bean
    public PersistenceExceptionTranslationPostProcessor exceptionTranslation() {
        return new PersistenceExceptionTranslationPostProcessor();
    }
    
}
