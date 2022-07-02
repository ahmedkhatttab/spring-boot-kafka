package com.sb.kafka.controller;

import com.sb.kafka.model.MessageEntity;
import com.sb.kafka.service.KafkaManualConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/consumers")
@Slf4j
public class ConsumerController {

    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;
    private final KafkaManualConsumerService manualConsumerService;


    @GetMapping("/manual")
    public List<MessageEntity> getAllMessages(@RequestParam(value = "partition", required = false, defaultValue = "0") Integer partition,
                                              @RequestParam(value= "offset", required = false, defaultValue = "0") Integer offset){

        List<Object> data = manualConsumerService.getAllMsg(FIRST_TOPIC, partition, offset);

        return data.stream().map(e-> (MessageEntity) e).collect(Collectors.toList());
    }

}
