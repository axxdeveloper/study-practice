package com.shooeugenesea.kafka;

import com.shooeugenesea.IntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.shooeugenesea.config.KafkaTopicConfig.TX_REQ_TOPIC_1;
import static com.shooeugenesea.config.KafkaTopicConfig.TX_REQ_TOPIC_2;
import static com.shooeugenesea.config.KafkaTopicConfig.TX_RES_TOPIC_1;
import static com.shooeugenesea.config.KafkaTopicConfig.TX_RES_TOPIC_2;

@SpringBootTest
public class KafkaManualCommitTransactionPerformanceTest extends IntegrationTest {

    private final CountDownLatch start = new CountDownLatch(1);
    private final Long msgCount = 1000L;
    private volatile long startTime = -1;
    private volatile long msgId = 0;
    private volatile long triggerCount = 0;
    private volatile long consum2Count = 0;
    private volatile long readProcessWriteCount = 0;
    private ScheduledExecutorService s = Executors.newScheduledThreadPool(20);
    
    @Test
    void testKafkaTransaction_2triggers_2producers_1consumer() throws ExecutionException, InterruptedException {
        asyncPringCount();
        asyncTrigger();
        asyncReadProcessWrite();
        start.countDown();
        syncReadResponseTopics();
    }

    private void asyncPringCount() {
        s.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                printCounts();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void syncReadResponseTopics() {
        KafkaConsumer<String, String> consumer = consumer(UUID.randomUUID().toString());
        consumer.subscribe(Arrays.asList(TX_RES_TOPIC_1, TX_RES_TOPIC_2));
        while (!s.isShutdown()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(record -> {
                consumer.commitAsync();
                if (consum2Count++ >= msgCount) {
                    System.out.println("Time Duration:" + Duration.ofMillis(System.currentTimeMillis() - startTime).getSeconds() + " Seconds");
                    printCounts();
                    s.shutdownNow();
                }
            });
        }
    }

    private void printCounts() {
        System.out.println("trigger:" + triggerCount + ", readProcessWrite:" + readProcessWriteCount + ", consume:" + consum2Count);
    }

    private void asyncReadProcessWrite() {
        String groupId = UUID.randomUUID().toString();
        asyncReadProcessWrite(groupId, TX_REQ_TOPIC_1, TX_RES_TOPIC_1); // read from request, write to response
        asyncReadProcessWrite(groupId, TX_REQ_TOPIC_2, TX_RES_TOPIC_2); // read from request 2, write to response 2
    }

    private void asyncReadProcessWrite(String groupId, String readTopic, String writeTopic) {
        s.execute(() -> {
            KafkaProducer<String, String> producer1 = producer(UUID.randomUUID().toString());
            KafkaConsumer<String, String> consumerForProducer1 = consumer(groupId, readTopic);
            while (!s.isShutdown()) {
                ConsumerRecords<String, String> records = consumerForProducer1.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    consumerForProducer1.commitAsync();
                    try {
                        producer1.beginTransaction();
                        producer1.send(new ProducerRecord<>(writeTopic, String.valueOf(record.value())));
                        producer1.commitTransaction();
                    } catch (Exception e) {
                        e.printStackTrace();
                        producer1.abortTransaction();
                    }
                    readProcessWriteCount++;
                });
            }
        });
    }

    private void asyncTrigger() {
        KafkaProducer<String, String> trigger1 = producer(UUID.randomUUID().toString());
        KafkaProducer<String, String> trigger2 = producer(UUID.randomUUID().toString());
        s.execute(() -> {
            await();
            markStartIfNotStartYet();
            while (!s.isShutdown()) {
                try {
                    trigger1.beginTransaction();
                    trigger1.send(new ProducerRecord<>(TX_REQ_TOPIC_1, String.valueOf(msgId++)));
                    triggerCount++;
                    trigger1.commitTransaction();
                } catch (Exception e) {
                    e.printStackTrace();
                    trigger1.abortTransaction();
                }
            }
            trigger1.close();
        });
        s.execute(() -> {
            await();
            markStartIfNotStartYet();
            while (!s.isShutdown()) {
                try {
                    trigger2.beginTransaction();
                    trigger2.send(new ProducerRecord<>(TX_REQ_TOPIC_2, String.valueOf(msgId++)));
                    triggerCount++;
                    trigger2.commitTransaction();
                } catch (Exception e) {
                    e.printStackTrace();
                    trigger2.abortTransaction();
                } 
            }
            trigger2.close();
        });
        
    }

    private void await() {
        try {
            start.await();
        } catch (InterruptedException e) {
        }
    }

    private void markStartIfNotStartYet() {
        if (startTime == -1) startTime = System.currentTimeMillis();
    }
    
    private KafkaProducer<String, String> producer(String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-" + UUID.randomUUID().toString());
        
        KafkaProducer producer = new KafkaProducer<>(props);
        producer.initTransactions();
        return producer;
    }

    private KafkaConsumer<String, String> consumer(String groupId, String...topics) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        
        
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topics));
        return consumer;
    }

}
