package examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConsumerMain {

    public static void main(String[] args) {
        int total = 0;
        Properties kafkaProps = kafkaProps("localhost:9092");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
//            consumer.subscribe(Arrays.asList(ProducerMain.TOPIC));
            consumer.assign(consumer.partitionsFor(ProducerMain.TOPIC).stream().map(p -> new TopicPartition(ProducerMain.TOPIC, p.partition())).collect(Collectors.toList()));

            resetOffset(consumer);
            total += consumeAllMessages(consumer);
            System.out.println("receive total " + total + " messages");

            resetOffset(consumer);
            total += consumeAllMessages(consumer);
            System.out.println("receive total " + total + " messages");
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    private static void resetOffset(KafkaConsumer<String, String> consumer) {
        consumer.partitionsFor(ProducerMain.TOPIC).forEach(p -> {
            consumer.seek(new TopicPartition(ProducerMain.TOPIC, p.partition()), 0);
        });
    }

    private static int consumeAllMessages(KafkaConsumer<String, String> consumer) {
        int total = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.count() > 0) {
                records.forEach(c -> System.out.println(c.value()));
                total += records.count();
                System.out.println("receive:" + records.count() + " messages.");
            } else {
                System.out.println("receive no message");
                break;
            }
        }
        return total;
    }

    private static Properties kafkaProps(String url) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestGroup5");
        return properties;
    }

}
