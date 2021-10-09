package com.shooeugenesea.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    public static final String REQUEST_TOPIC = "requestTopic";
    public static final String RESPONSE_TOPIC = "responseTopic";
    public static final String RESPONSE_TOPIC_2 = "responseTopic2";

    public static final String TX_REQ_TOPIC_1 = "txReqTopic1";
    public static final String TX_REQ_TOPIC_2 = "txReqTopic2";
    public static final String TX_RES_TOPIC_1 = "txResTopic1";
    public static final String TX_RES_TOPIC_2 = "txResTopic2";
    

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }
    
    @Bean
    public NewTopic txReqTopic1() {
        return new NewTopic(TX_REQ_TOPIC_1, 2, (short)1);
    }
    
    @Bean
    public NewTopic txReqTopic2() {
        return new NewTopic(TX_REQ_TOPIC_2, 2, (short)1);
    }
    
    @Bean
    public NewTopic txResTopic1() {
        return new NewTopic(TX_RES_TOPIC_1, 2, (short)1);
    }
    
    @Bean
    public NewTopic txResTopic2() {
        return new NewTopic(TX_RES_TOPIC_2, 2, (short)1);
    }
    
    @Bean
    public NewTopic requestTopic() {
         return new NewTopic(REQUEST_TOPIC, 2, (short) 1);
    }
    
    @Bean
    public NewTopic responseTopic() {
         return new NewTopic(RESPONSE_TOPIC, 2, (short) 1);
    }
    
    @Bean
    public NewTopic responseTopic2() {
         return new NewTopic(RESPONSE_TOPIC_2, 2, (short) 1);
    }

}
