package com.shooeugenesea.kafka;

import com.shooeugenesea.entity.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessageSender {

    private KafkaTemplate kafkaTemplate;
    
    @Autowired
    public MessageSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(Person person, String topic) {
        kafkaTemplate.send(topic, person.getId().toString());
    }

}
