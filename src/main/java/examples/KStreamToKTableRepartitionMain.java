package examples;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KStreamToKTableRepartitionMain {

    private static final String TOPIC_STOCK = KStreamToKTableRepartitionMain.class.getName() + "_stock2";
    private static final String TOPIC_PRICE = KStreamToKTableRepartitionMain.class.getName() + "_price2";
    private static final String STOCK_VT_KEY = "VT";
    public static final String STOCK_VT_NAME = "Vanguard Total World Stock ETF";

    public static void main(String[] args) {
        try {
            Properties kafkaProps = kafkaProps();
            createTopicIfNotExist(kafkaProps, TOPIC_STOCK, TOPIC_PRICE);
        
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> priceStream = builder.stream(TOPIC_PRICE, Consumed.with(Serdes.String(), Serdes.String()));
            KTable<String, String> stockTable = builder.table(TOPIC_STOCK, Consumed.with(Serdes.String(), Serdes.String()));
            
            priceStream.join(stockTable, (price, stockName) -> price + "/" + stockName)
                            .print(Printed.toSysOut());
            
            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kafkaProps());
            kafkaStreams.start();   

//            sendNewStock(kafkaProps, STOCK_VT_KEY, STOCK_VT_NAME);
//            sendStockPrices(kafkaProps, STOCK_VT_KEY, IntStream.range(0,100).mapToObj(i -> String.valueOf(i)).toArray(String[]::new));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
        
    private static void sendNewStock(Properties kafkaProps, String stockKey, String stockName) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps, Serdes.String().serializer(), Serdes.String().serializer())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_STOCK, stockKey, stockName);
            System.out.println("offset:" + producer.send(record).get(10, TimeUnit.SECONDS).offset());
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }    
        
    private static void sendStockPrices(Properties kafkaProps, String stockKey, String...prices) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps, Serdes.String().serializer(), Serdes.String().serializer())) {
            for (String price: prices) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_PRICE, "", price);
                System.out.println("offset:" + producer.send(record).get(10, TimeUnit.SECONDS).offset());
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    private static void createTopicIfNotExist(Properties kafkaProps, String...topics) throws InterruptedException, ExecutionException, TimeoutException {
        try (AdminClient adminClient = AdminClient.create(kafkaProps)) {
            List<NewTopic> newTopics = Arrays.stream(topics)
                                            .map(topic -> new NewTopic(topic, 2, (short)1))
                                            .collect(Collectors.toList());
            adminClient.createTopics(newTopics).all().get(10, TimeUnit.SECONDS);
            System.out.println("create topics:" + newTopics);
        } catch (Exception ex) {}
    }
    
    private static Properties kafkaProps() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestGroup5");
        return properties;
    }
}
