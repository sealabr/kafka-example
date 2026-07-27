package com.example.subscription.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentChargedEvent(
        String subscriptionId,
        String customerId,
        BigDecimal amount,
        Instant chargedAt
) {
}
