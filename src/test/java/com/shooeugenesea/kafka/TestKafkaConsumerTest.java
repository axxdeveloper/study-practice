package com.shooeugenesea.kafka;

import com.shooeugenesea.IntegrationTest;
import com.shooeugenesea.config.KafkaTopicConfig;
import com.shooeugenesea.dao.PersonDao;
import com.shooeugenesea.entity.Person;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class) // inject PersonDao
@SpringBootTest
public class TestKafkaConsumerTest extends IntegrationTest {

    @Autowired
    private PersonDao personDao;

    @Test
    public void test() throws InterruptedException, ExecutionException {
        String name = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        final Consumer<String, String> consumer = consumer();
        ConsumerRecords<String, String> records = null;
        producer().send(new ProducerRecord<>(KafkaTopicConfig.RESPONSE_TOPIC, "", name)).get();
        while (latch.getCount() > 0 && !(records = consumer.poll(Duration.ofSeconds(10))).isEmpty()) {
            records.forEach(record -> {
                System.out.println(record.value());
                String id = record.value();
                Optional<Person> optPerson = personDao.findById(UUID.fromString(id));
                if (optPerson.isPresent() && name.equals(optPerson.get().getName())) {
                    latch.countDown();
                }
            });
        }
        Assert.assertTrue(latch.await(20, TimeUnit.SECONDS));
    }
    
    private static Producer<String, String> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "hardCodedClientId");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    
    private static Consumer<String, String> consumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hardCodedGroupId");
        
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaTopicConfig.REQUEST_TOPIC));
        return consumer;
      }
    
    
}
