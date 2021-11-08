package examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Properties;
import java.util.UUID;

public class ProcessorAPIMain {

    public static void main(String[] args) {
        Topology topology = new Topology();
        topology.addSource("MyTopicSource", ProducerMain.TOPIC);
        topology.addProcessor("MyTopicProcess", MyTopicProcessor::new, "MyTopicSource");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, kafkaProps("localhost:9092"));
        kafkaStreams.start();
    }

    private static Properties kafkaProps(String url) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return properties;
    }

    public static class MyTopicProcessor implements Processor<String, String> {

        @Override
        public void init(ProcessorContext processorContext) {
            System.out.println(processorContext);
        }

        @Override
        public void process(String k, String v) {
            System.out.println("Process - " + k + ":" + v);            
        }

        @Override
        public void close() {

        }
    }
}
