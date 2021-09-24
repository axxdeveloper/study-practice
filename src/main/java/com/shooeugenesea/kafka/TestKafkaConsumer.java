package com.shooeugenesea.kafka;

import com.shooeugenesea.config.KafkaTopicConfig;
import com.shooeugenesea.dao.PersonDao;
import com.shooeugenesea.entity.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class TestKafkaConsumer {
    
    private KafkaTemplate<String, String> kafkaTemplate;
    private PersonDao personDao;
    
    @Autowired
    public TestKafkaConsumer(KafkaTemplate<String, String> kafkaTemplate, PersonDao personDao) {
        this.kafkaTemplate = kafkaTemplate;
        this.personDao = personDao;
    }
    
    @KafkaListener(topics = KafkaTopicConfig.RESPONSE_TOPIC, groupId = "anyGroupId")
    public void listenGroupFoo(@Payload String message) {
        System.out.println("Received Message: " + message);
        Person persisted = personDao.save(new Person(message));
        kafkaTemplate.send(KafkaTopicConfig.REQUEST_TOPIC, persisted.getId().toString());
    }

}
