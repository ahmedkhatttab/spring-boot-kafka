package com.sb.kafka.service;


import com.sb.kafka.model.MessageEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaCustomConsumerService {

//    @KafkaListener(topics = {"${kafka.first-topic}"}, containerFactory = "customKafkaListener")
//    public void listenToFirstTopic(Object msg){
//        log.info(":::::::::::::::::::::::::");
//        log.info("Consumer A :{}" , msg);
//    }
//
//
//    @KafkaListener(topics = {"${kafka.first-topic}"}, containerFactory = "customKafkaListener")
//    public void listenToFirstTopic2(MessageEntity msg){
//        log.info(":::::::::::::::::::::::::");
//        log.info("Consumer B :{}" , msg);
//    }
//
//
//    @KafkaListener(topics = {"${kafka.first-topic}"}, groupId = "det_kafka_cg",containerFactory = "customKafkaListener")
//    public void listenToFirstTopic3(
//                                    ConsumerRecord<String, MessageEntity> consumerRecord,
//                                    @Payload MessageEntity message,
//                                    @Header(KafkaHeaders.OFFSET) int offset,
//                                    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int received_partition_id
//    ){
//        log.info(":::::::::::::::::::::::::");
//        log.info("Message CONSUMER_RECORD :{}" , consumerRecord);
//        log.info("Message Payload :{}" , message);
//        log.info("Message RECEIVED_PARTITION_ID :{}" , received_partition_id);
//        log.info("Message OFFSET :{}" , offset);
//        log.info(":::::::::::::::::::::::::");
//    }



    @KafkaListener
    (
        topicPartitions =
            @TopicPartition
            (
                topic = "${kafka.first-topic}",
                partitionOffsets = {
                    @PartitionOffset(partition = "0", initialOffset = "0"),
                    @PartitionOffset(partition = "2", initialOffset = "0"),
                    @PartitionOffset(partition = "3", initialOffset = "0")
                }
            ),
        groupId = "all_kafka2_cg",
        containerFactory = "customKafkaListener"
    )
//    @KafkaListener
//    (
//        groupId = "all_kafka2_cg",
//        containerFactory = "customKafkaListener",
//        topicPartitions =
//            @TopicPartition
//            (
//                topic = "${kafka.first-topic}",
//                partitionOffsets =
//                {
//                        @PartitionOffset(partition = "2", initialOffset = "0")
//                }
//            )
//    )
    public void listenToAllTopics(MessageEntity msg){
        log.info("::::::::::::ALL MSG:::::::::::::");
            log.info(msg.toString());
        log.info("::::::::::::ALL MSG:::::::::::::");
    }






}
