package com.example.banking.event;

import java.time.Instant;

public record TransferSettledEvent(
        String transferId,
        Instant settledAt
) {
}
