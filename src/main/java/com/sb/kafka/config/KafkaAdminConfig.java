package com.sb.kafka.config;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import javax.annotation.PostConstruct;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaAdminConfig {

    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;

    private final KafkaAdmin kafkaAdmin;

    public NewTopic createTopic(String topicName, int partitionsCount, int replicasCount, String retentionPeriodInMS){
        return
                TopicBuilder
                    .name(topicName)
                    .partitions(partitionsCount)
                    .replicas(replicasCount)
                    .config(TopicConfig.RETENTION_MS_CONFIG, retentionPeriodInMS) // retention time is 5 minutes
                    .build();
    }


    @PostConstruct
    public void init(){
        kafkaAdmin.setBootstrapServersSupplier(()->"localhost:29092");
        kafkaAdmin.createOrModifyTopics(createTopic(FIRST_TOPIC,4, 1, "300000"));
    }


}
