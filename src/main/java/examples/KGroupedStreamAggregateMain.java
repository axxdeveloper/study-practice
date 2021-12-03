package examples;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KGroupedStreamAggregateMain {

    private static final String TOPIC_STOCK = KGroupedStreamAggregateMain.class.getName() + "_stock_" + UUID.randomUUID().toString();
    private static final String TOPIC_PRICE = KGroupedStreamAggregateMain.class.getName() + "_price_" + UUID.randomUUID().toString();
    private static final String STOCK_VT_KEY = "VT";
    public static final String STOCK_VT_NAME = "Vanguard Total World Stock ETF";
    
    public static class HighPrice {
        private final TreeSet<Integer> treeSet = new TreeSet<>();

        @Override
        public String toString() {
            return "HighPrice{" +
                    "treeSet=" + treeSet +
                    '}';
        }
    }

    public static void main(String[] args) {
        try {
            Properties kafkaProps = kafkaProps();
            createTopicIfNotExist(kafkaProps, TOPIC_STOCK, TOPIC_PRICE);

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, Integer> priceStream = builder.stream(TOPIC_PRICE, Consumed.with(Serdes.String(), Serdes.Integer()));
            priceStream.groupByKey(Serialized.with(Serdes.String(), Serdes.Integer()))
                        .aggregate(() -> 0, (key, value, aggregate) -> aggregate + value, 
                                Materialized.with(Serdes.String(), Serdes.Integer())) // specify aggregate type
                        .toStream().foreach((k,v) -> System.out.println("aggregate result => " + k + ":" + v));
            
            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kafkaProps());
            kafkaStreams.start();
            new Thread(() -> sendStockPrices(kafkaProps, STOCK_VT_KEY, IntStream.range(0,100).toArray())).start();
            System.out.println("store");
            ReadOnlyKeyValueStore<String, Integer> store = waitUntilStoreIsQueryable(TOPIC_PRICE, QueryableStoreTypes.keyValueStore(), kafkaStreams);
            store.all().forEachRemaining(kv -> System.out.println("store => " + kv.key + ":" + kv.value));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                  final QueryableStoreType<T> queryableStoreType,
                                                  final KafkaStreams streams) throws InterruptedException {
        int times = 0;
        while (times++ < 1000000) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                System.out.println("wait 100 ms. times:" + times);
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
        throw new RuntimeException("Out of times");
    }

    private static void sendStockPrices(Properties kafkaProps, String stockKey, int...prices) {
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(kafkaProps, Serdes.String().serializer(), Serdes.Integer().serializer())) {
            for (int price: prices) {
                TimeUnit.MILLISECONDS.sleep(500);
                ProducerRecord<String, Integer> record = new ProducerRecord<>(TOPIC_PRICE, stockKey + (price % 5), price);
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
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestGroup5");
        return properties;
    }
}
