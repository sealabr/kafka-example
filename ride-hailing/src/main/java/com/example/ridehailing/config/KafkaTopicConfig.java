package com.example.ridehailing.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic rideRequestedTopic(@Value("${ride-hailing.topics.ride-requested}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic driverAssignedTopic(@Value("${ride-hailing.topics.driver-assigned}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic rideCompletedTopic(@Value("${ride-hailing.topics.ride-completed}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }
}
