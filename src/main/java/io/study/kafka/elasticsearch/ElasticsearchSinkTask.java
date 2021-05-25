package io.study.kafka.elasticsearch;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class ElasticsearchSinkTask extends SinkTask {
    
    private ElasticsearchSinkConnectorConfig config;
    private RestHighLevelClient esClient;
    
    @Override
    public String version() {
        return "1.0";
    }
    
    @Override
    public void start(Map<String, String> props) {
        try {
            config = new ElasticsearchSinkConnectorConfig(props);
        }
        catch(ConfigException e) {
            log.error(e.getMessage(), e);
        }
        
        esClient = new RestHighLevelClient(RestClient.builder(new HttpHost(config.getString(config.ES_CLUSTER_HOST),
                                                                           config.getInt(config.ES_CLUSTER_PORT))));
    }
    
    @Override
    public void put(Collection<SinkRecord> records) {
        if(records.size() > 0) {
            BulkRequest bulkRequest = new BulkRequest();
            for(SinkRecord record : records) {
                Gson gson = new Gson();
                Map map = gson.fromJson(record.value().toString(), Map.class);
                bulkRequest.add(new IndexRequest(config.getString(config.ES_INDEX)).source(map, XContentType.JSON));
                log.info("record : {}", record.value());
            }
            esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                
                @Override
                public void onResponse(BulkResponse bulkItemResponse) {
                    if(bulkItemResponse.hasFailures()) {
                        log.error(bulkItemResponse.buildFailureMessage());
                    }
                    else {
                        log.info("bulk save success !");
                    }
                }
                
                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        
    }
    
    @Override
    public void stop() {
        try {
            esClient.close();
        }
        catch(IOException e) {
            log.error(e.getMessage(), e);
        }
    }
    
}
