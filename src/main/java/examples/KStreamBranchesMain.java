package examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class KStreamBranchesMain {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(ProducerMain.TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String>[] branches = stream.branch((k,v) -> v.indexOf('1') > -1, (k,v) -> v.indexOf('2') > -1);
        branches[0].foreach((k,v) -> System.out.println("branch[0]:" + v));
        branches[1].foreach((k,v) -> System.out.println("branch[1]:" + v));
        
        branches[0].mapValues(v -> v + "-filtered").merge(branches[1]).foreach((k,v) -> System.out.println(v));
        System.out.println("===============> flatMapValues =============>");
        stream.flatMapValues(v -> Arrays.asList(v,v)).foreach((k,v) -> System.out.println(k + ":" + v));
        
        System.out.println("===============> flatMap =============>");
        stream.flatMap((k,v) -> Arrays.asList(KeyValue.pair(k+"/"+k,v+"/"+v))).foreach((k, v) -> System.out.println(k + ":" + v));
        
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
