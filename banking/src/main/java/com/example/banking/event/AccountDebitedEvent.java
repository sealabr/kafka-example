package com.example.banking.event;

import java.math.BigDecimal;
import java.time.Instant;

public record AccountDebitedEvent(
        String transferId,
        String accountId,
        BigDecimal amount,
        Instant debitedAt
) {
}
