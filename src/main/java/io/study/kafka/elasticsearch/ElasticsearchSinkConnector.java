package io.study.kafka.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ElasticsearchSinkConnector extends SinkConnector {
    
    private Map<String, String> configProperties;
    
    @Override
    public String version() {
        return "1.0";
    }
    
    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try {
            new ElasticsearchSinkConnectorConfig(props);
        }
        catch(ConfigException e) {
            log.error(e.getMessage(), e);
        }
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return ElasticsearchSinkTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for(int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }
    
    @Override
    public ConfigDef config() {
        return ElasticsearchSinkConnectorConfig.CONFIG;
    }
    
    @Override
    public void stop() {
        log.info("Stop elasticsearch connector.");
    }
    
}
