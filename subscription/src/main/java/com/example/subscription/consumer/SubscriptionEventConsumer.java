package com.example.subscription.consumer;

import com.example.subscription.event.PaymentChargedEvent;
import com.example.subscription.event.SubscriptionCancelledEvent;
import com.example.subscription.event.SubscriptionCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SubscriptionEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionEventConsumer.class);

    @KafkaListener(topics = "${subscription.topics.subscription-created}", groupId = "${spring.kafka.consumer.group-id}")
    public void onSubscriptionCreated(SubscriptionCreatedEvent event) {
        log.info("Consumed subscription.created: {}", event);
    }

    @KafkaListener(topics = "${subscription.topics.payment-charged}", groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentCharged(PaymentChargedEvent event) {
        log.info("Consumed payment.charged: {}", event);
    }

    @KafkaListener(topics = "${subscription.topics.subscription-cancelled}", groupId = "${spring.kafka.consumer.group-id}")
    public void onSubscriptionCancelled(SubscriptionCancelledEvent event) {
        log.info("Consumed subscription.cancelled: {}", event);
    }
}
