package com.example.subscription.event;

import java.time.Instant;

public record SubscriptionCancelledEvent(
        String subscriptionId,
        String customerId,
        Instant cancelledAt
) {
}
