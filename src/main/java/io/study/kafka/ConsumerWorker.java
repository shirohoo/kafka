package io.study.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.tomcat.util.buf.StringUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConsumerWorker implements Runnable {
    
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>();
    
    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties props;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;
    
    public ConsumerWorker(Properties props, String topic, int num) {
        log.info("Generate ConsumerWorker !");
        this.props = props;
        this.topic = topic;
        this.threadName = "consumer-thread-" + num;
    }
    
    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String, String> record : records) {
                    System.out.println("record = " + record.value());
                    addHdfsFileBuffer(record);
                }
                saveBufferToHdfsFile(consumer.assignment());
            }
        }
        catch(WakeupException e) {
            log.warn("Wakeup consumer !");
        }
        catch(Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            consumer.close();
        }
        
    }
    
    private void addHdfsFileBuffer(ConsumerRecord<String, String> record) {
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);
        
        if(buffer.size() == 1) {
            currentFileOffset.put(record.partition(), record.offset());
        }
    }
    
    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
        partitions.forEach(partition->checkFlushCount(partition.partition()));
    }
    
    private void checkFlushCount(int partitionNum) {
        if(bufferString.get(partitionNum) != null) {
            if(bufferString.get(partitionNum).size() > FLUSH_RECORD_COUNT - 1) {
                save(partitionNum);
            }
        }
    }
    
    private void save(int partitionNum) {
        if(bufferString.get(partitionNum).size() > 0) {
            try {
                String fileName = "C:\\log_test\\kafka_logs-" + partitionNum + "-" + currentFileOffset.get(partitionNum) + ".log";
                FileOutputStream outputStream = new FileOutputStream(fileName);
                
                byte[] content = StringUtils.join(bufferString.get(partitionNum), '\n')
                                            .getBytes(StandardCharsets.UTF_8);
                
                outputStream.write(content);
                outputStream.flush();
                outputStream.close();
                
                bufferString.put(partitionNum, new ArrayList<>());
            }
            catch(FileNotFoundException e) {
                log.error(e.getMessage(), e);
            }
            catch(IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    private void saveRemainBufferToHdfsFile() {
        bufferString.forEach((partitionNum, value)->this.save(partitionNum));
    }
    
    public void stopAndWakeup() {
        log.info("Stop and Wakeup !");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }
    
}
