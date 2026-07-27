package com.example.banking.event;

import java.math.BigDecimal;
import java.time.Instant;

public record TransferInitiatedEvent(
        String transferId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        Instant initiatedAt
) {
}
