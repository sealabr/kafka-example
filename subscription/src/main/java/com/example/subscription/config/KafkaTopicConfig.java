package com.example.subscription.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic subscriptionCreatedTopic(@Value("${subscription.topics.subscription-created}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic paymentChargedTopic(@Value("${subscription.topics.payment-charged}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic subscriptionCancelledTopic(@Value("${subscription.topics.subscription-cancelled}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }
}
