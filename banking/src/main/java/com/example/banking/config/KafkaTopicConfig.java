package com.example.banking.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic transferInitiatedTopic(@Value("${banking.topics.transfer-initiated}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic transferSettledTopic(@Value("${banking.topics.transfer-settled}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic accountDebitedTopic(@Value("${banking.topics.account-debited}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }
}
