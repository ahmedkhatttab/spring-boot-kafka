package com.sb.kafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
//@EnableKafka
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String PRODUCER_BROKER;

    private ConsumerFactory<String, Object> consumerFactory(String groupId){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PRODUCER_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("com.sb.kafka.model");


        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), jsonDeserializer);
    }



    // case 1: manual consumer
    @Bean
    public Consumer<String, Object> manualConsumer(){
        return consumerFactory("custom_kl_cg").createConsumer();
    }




    // case 2: custom kafka listener
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> customKafkaListener(){
        ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListener = new ConcurrentKafkaListenerContainerFactory<>();

        kafkaListener.setConsumerFactory(consumerFactory("manual_kl_cg"));
//        kafkaListener.setConcurrency(1);
//        kafkaListener.setAutoStartup(true);

        return kafkaListener;
    }

}
