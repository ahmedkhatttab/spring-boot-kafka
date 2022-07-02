package com.sb.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaManualConsumerService {

    private final Consumer<String, Object> manualConsumer;

    public List<Object> getAllMsg(String topic, int partition, int offset){

        TopicPartition topicPartition= new TopicPartition(topic, partition);

        manualConsumer.assign(List.of(topicPartition));

        manualConsumer.seek(topicPartition, offset);

        ConsumerRecords<String, Object> consumerRecords = manualConsumer.poll(Duration.ofMillis(1000));

        consumerRecords.forEach(r -> {
            log.info("__________________________________");
            log.info("RECORDE: {}", r.value());
            log.info("__________________________________");
        });

        manualConsumer.unsubscribe();

        return
                StreamSupport
                .stream(consumerRecords.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
    }


}
