package com.sb.kafka.controller;


import com.sb.kafka.model.MessageEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/producers")
@RequiredArgsConstructor
@Slf4j
public class ProducerController {

    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping
    public void createNewMsg(@RequestBody MessageEntity msg) throws ExecutionException, InterruptedException {

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(FIRST_TOPIC, msg.getType(), msg);

        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(producerRecord);

        SendResult<String, Object> result = future.get();

        log.info("________________________________________________________");

        log.info("Partition NO #"+result.getRecordMetadata().partition());

        log.info("OFFSET NO #"+result.getRecordMetadata().offset() + " HASH OFFSET: "+ result.getRecordMetadata().hasOffset());

        log.info("________________________________________________________");
    }

}
