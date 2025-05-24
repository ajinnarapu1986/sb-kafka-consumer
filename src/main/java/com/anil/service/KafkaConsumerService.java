package com.anil.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaConsumerService {

	private static final String TOPIC = "test-topic2";

	
	@KafkaListener(topics = TOPIC, groupId = "my-consumer-group")
	public void receiveMessage(Message<String> message) {
		log.info("Received message: {}", message);
		
		
		 // Get the payload (message content)
        String payload = message.getPayload();
        
        // Get metadata
        String topic = message.getHeaders().get("kafka_receivedTopic").toString();
        Integer partition = (Integer) message.getHeaders().get("kafka_receivedPartitionId");
        Long offset = (Long) message.getHeaders().get("kafka_offset");
        
        // Print metadata and payload
        System.out.println("Received Message: " + payload);
        System.out.println("Topic: " + topic);
        System.out.println("Partition: " + partition);
        System.out.println("Offset: " + offset);
		
	}
}
