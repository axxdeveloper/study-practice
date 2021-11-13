package examples;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ProducerMain {

    public static final String TOPIC = "mytopic";
    public static final String TOPIC_TARGET = "my-target-topic"; 
    public static int MSG_COUNT = 100;

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        Properties kafkaProps = kafkaProps("localhost:9092");
        createTopicIfNotExist(kafkaProps, TOPIC);
        createTopicIfNotExist(kafkaProps, TOPIC_TARGET);
        sendMessages(kafkaProps, TOPIC);
    }

    private static void sendMessages(Properties kafkaProps, String topic) {
        for (int i = 0; i < MSG_COUNT; i++) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "mykey", "myval" + i);
                System.out.println("offset:" + producer.send(record).get(10, TimeUnit.SECONDS).offset());
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void createTopicIfNotExist(Properties kafkaProps, String topic) throws InterruptedException, ExecutionException, TimeoutException {
        try (AdminClient adminClient = AdminClient.create(kafkaProps)) {
            NewTopic newTopic = new NewTopic(topic, 1, (short)1);
            if ( adminClient.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic) ) {
                System.out.println("Topic " + topic + " already exist");
                return;
            }
            System.out.println("Topic " + topic + " doesn't exist, create it");
            List<NewTopic> newTopics = new ArrayList<>();
            newTopics.add(newTopic);
            adminClient.createTopics(newTopics).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static Properties kafkaProps(String url) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
