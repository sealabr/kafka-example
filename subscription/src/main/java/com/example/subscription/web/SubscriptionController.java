package com.example.subscription.web;

import com.example.subscription.event.PaymentChargedEvent;
import com.example.subscription.event.SubscriptionCancelledEvent;
import com.example.subscription.event.SubscriptionCreatedEvent;
import com.example.subscription.producer.SubscriptionEventProducer;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/subscriptions")
public class SubscriptionController {

    private final SubscriptionEventProducer subscriptionEventProducer;

    public SubscriptionController(SubscriptionEventProducer subscriptionEventProducer) {
        this.subscriptionEventProducer = subscriptionEventProducer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> createSubscription(@RequestBody SubscriptionRequest request) {
        String subscriptionId = UUID.randomUUID().toString();
        BigDecimal amount = planAmount(request.plan());

        subscriptionEventProducer.publishSubscriptionCreated(new SubscriptionCreatedEvent(
                subscriptionId,
                request.customerId(),
                request.plan(),
                Instant.now()));

        subscriptionEventProducer.publishPaymentCharged(new PaymentChargedEvent(
                subscriptionId,
                request.customerId(),
                amount,
                Instant.now()));

        return Map.of(
                "subscriptionId", subscriptionId,
                "status", "events-published");
    }

    @PostMapping("/{subscriptionId}/cancel")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> cancelSubscription(
            @PathVariable String subscriptionId,
            @RequestBody CancelRequest request) {
        subscriptionEventProducer.publishSubscriptionCancelled(new SubscriptionCancelledEvent(
                subscriptionId,
                request.customerId(),
                Instant.now()));

        return Map.of(
                "subscriptionId", subscriptionId,
                "status", "cancel-event-published");
    }

    private static BigDecimal planAmount(String plan) {
        if (plan == null) {
            return new BigDecimal("9.90");
        }
        return switch (plan.toUpperCase()) {
            case "PRO" -> new BigDecimal("29.90");
            case "ENTERPRISE" -> new BigDecimal("99.90");
            default -> new BigDecimal("9.90");
        };
    }

    public record SubscriptionRequest(String customerId, String plan) {
    }

    public record CancelRequest(String customerId) {
    }
}
