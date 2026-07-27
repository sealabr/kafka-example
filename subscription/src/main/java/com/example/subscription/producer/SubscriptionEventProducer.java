package com.example.subscription.producer;

import com.example.subscription.event.PaymentChargedEvent;
import com.example.subscription.event.SubscriptionCancelledEvent;
import com.example.subscription.event.SubscriptionCreatedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class SubscriptionEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String subscriptionCreatedTopic;
    private final String paymentChargedTopic;
    private final String subscriptionCancelledTopic;

    public SubscriptionEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${subscription.topics.subscription-created}") String subscriptionCreatedTopic,
            @Value("${subscription.topics.payment-charged}") String paymentChargedTopic,
            @Value("${subscription.topics.subscription-cancelled}") String subscriptionCancelledTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.subscriptionCreatedTopic = subscriptionCreatedTopic;
        this.paymentChargedTopic = paymentChargedTopic;
        this.subscriptionCancelledTopic = subscriptionCancelledTopic;
    }

    public void publishSubscriptionCreated(SubscriptionCreatedEvent event) {
        kafkaTemplate.send(subscriptionCreatedTopic, event.subscriptionId(), event);
    }

    public void publishPaymentCharged(PaymentChargedEvent event) {
        kafkaTemplate.send(paymentChargedTopic, event.subscriptionId(), event);
    }

    public void publishSubscriptionCancelled(SubscriptionCancelledEvent event) {
        kafkaTemplate.send(subscriptionCancelledTopic, event.subscriptionId(), event);
    }
}
