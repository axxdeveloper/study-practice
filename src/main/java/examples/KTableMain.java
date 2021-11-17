package examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.UUID;

public class KTableMain {

    public static void main(String[] args) {
        try {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> kStream = builder.stream(ProducerMain.TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
            kStream.peek((k,v) -> System.out.println("stream => " + k + ":" + v)).to(ProducerMain.TOPIC_TARGET, Produced.with(Serdes.String(), Serdes.String()));
            
            KTable<String, String> table = builder.table(ProducerMain.TOPIC_TARGET, Consumed.with(Serdes.String(), Serdes.String()));
            table.toStream().peek((k,v) -> System.out.println("peek table => " + k + ":" + v)).print(Printed.toSysOut());
            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kafkaProps());
            kafkaStreams.start();   
        } catch (Exception ex) {
            ex.printStackTrace();
        }
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
