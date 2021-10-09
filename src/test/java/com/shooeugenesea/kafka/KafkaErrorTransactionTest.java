package com.shooeugenesea.kafka;

import com.shooeugenesea.IntegrationTest;
import com.shooeugenesea.config.KafkaTopicConfig;
import com.shooeugenesea.dao.PersonDao;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.shooeugenesea.config.KafkaTopicConfig.RESPONSE_TOPIC;
import static com.shooeugenesea.config.KafkaTopicConfig.RESPONSE_TOPIC_2;

@SpringBootTest
@ContextConfiguration(initializers = KafkaErrorTransactionTest.ThrowErrorInitializer.class)
public class KafkaErrorTransactionTest extends IntegrationTest {

    @Autowired
    private PersonDao personDao;
    
    public static class ThrowErrorInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        
        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                    applicationContext,
                    "throw.error.enable=true"
            );
        }
    }
    
    @Test
    void testKafkaTransaction_noMessageWillBeSent() throws ExecutionException, InterruptedException {
        String name = UUID.randomUUID().toString();
        final Consumer<String, String> consumer = consumer();
        ConsumerRecords<String, String> records = null;
        producer().send(new ProducerRecord<>(KafkaTopicConfig.REQUEST_TOPIC, "", name)).get();
        Set<String> topics = new HashSet<>(Arrays.asList(RESPONSE_TOPIC, RESPONSE_TOPIC_2));
        Assert.assertEquals(2, topics.size());
        CountDownLatch latch = new CountDownLatch(topics.size());
        while (latch.getCount() > 0 && consumer.poll(Duration.ofSeconds(10)).isEmpty()) {
            latch.countDown();
        }
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private Producer<String, String> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "hardCodedClientId");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private Consumer<String, String> consumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hardCodedGroupId");
        
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(RESPONSE_TOPIC, RESPONSE_TOPIC_2));
        return consumer;
    }

}
