package io.study.kafka.config;

import com.p6spy.engine.spy.P6SpyOptions;
import io.study.kafka.formatter.P6spyPrettySqlFormatter;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class P6spyConfig {
    
    @PostConstruct
    public void setLogMessageFormat() {
        P6SpyOptions.getActiveInstance().setLogMessageFormat(P6spyPrettySqlFormatter.class.getName());
    }
    
}