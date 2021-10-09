package com.shooeugenesea.kafka;

import com.shooeugenesea.config.KafkaTopicConfig;
import com.shooeugenesea.dao.PersonDao;
import com.shooeugenesea.entity.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

//@Service
public class MyKafkaConsumer {
    
    private KafkaTemplate<String, String> kafkaTemplate;
    private PersonDao personDao;
    private MessageSender messageSender;
    
    @Value("${throw.error.enable:false}")
    private boolean throwErrorEnable = false;
    
    @Autowired
    public MyKafkaConsumer(KafkaTemplate<String, String> kafkaTemplate, PersonDao personDao, MessageSender messageSender) {
        this.kafkaTemplate = kafkaTemplate;
        this.personDao = personDao;
        this.messageSender = messageSender;
    }

    @KafkaListener(topics = KafkaTopicConfig.REQUEST_TOPIC, groupId = "anyGroupId")
    public void consumeMessage(@Payload String message) {
        System.out.println("Received Message: " + message);
        Person persisted = personDao.save(new Person(message));
        sendMessage(persisted);
    }
    
    void sendMessage(Person persisted) {
        kafkaTemplate.executeInTransaction(it -> {
            sendMessageTopic1(persisted, KafkaTopicConfig.RESPONSE_TOPIC);
            sendMessageTopic1(persisted, KafkaTopicConfig.RESPONSE_TOPIC_2);
            if (throwErrorEnable) {
                throw new RuntimeException("fake error");
            }
            return true;
        });
    }

    private void sendMessageTopic1(Person persisted, String responseTopic) {
        messageSender.sendMessage(persisted, responseTopic);
    }

}
