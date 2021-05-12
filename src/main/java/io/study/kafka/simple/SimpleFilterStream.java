package io.study.kafka.simple;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class SimpleFilterStream {
    
    private final static String APPLICATION_NAME = "streams-filter-application";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String STREAM_TEST = "stream_test";
    private final static String STREAM_TEST_FILTER = "stream_test_filter";
    
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        /**
         * 소스 프로세서: stream()
         * 스트림 프로세서: filter() 등
         * 싱크 프로세서: to()
         */
        
        /**
         * Topology를 정의
         * STREAM_LOG and STREAM_LOG_COPY
         * KStream = StreamsBuilder().stream()
         * KTable = StreamsBuilder().table()
         * GlobalKTable = StreamsBuilder().globalTable()
         */
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream(STREAM_TEST);
        
        /**
         * 싱크 프로세서: 필터
         * - 메시지의 길이가 5글자 이상인 것만 통과
         * 데이터의 흐름을 정의: STREAM_LOG to STREAM_LOG_COPY
         */
        kStream.filter(((key, value)->value.length() > 5))
               .to(STREAM_TEST_FILTER);
        
        /**
         * Topology를 연결하는 Stream을 정의
         */
        KafkaStreams stream = new KafkaStreams(builder.build(), properties);
        
        /**
         * 데이터 전송 시작
         */
        stream.start();
    }
    
}
