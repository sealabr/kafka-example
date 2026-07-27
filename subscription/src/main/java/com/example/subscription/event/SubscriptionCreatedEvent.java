package com.example.subscription.event;

import java.time.Instant;

public record SubscriptionCreatedEvent(
        String subscriptionId,
        String customerId,
        String plan,
        Instant createdAt
) {
}
