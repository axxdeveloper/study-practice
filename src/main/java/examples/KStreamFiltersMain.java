package examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.UUID;

public class KStreamFiltersMain {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(ProducerMain.TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        stream
            .filterNot((k,v) -> k.length() == 0)
            .filter((k,v) -> v.indexOf('2') > -1)
            .foreach((k,v) -> {
            System.out.println(k + ":" + v);
        });
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kafkaProps("localhost:9092"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Properties kafkaProps(String url) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        return properties;
    }

}
