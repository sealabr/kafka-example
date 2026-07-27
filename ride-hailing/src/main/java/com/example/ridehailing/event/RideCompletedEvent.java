package com.example.ridehailing.event;

import java.time.Instant;

public record RideCompletedEvent(
        String rideId,
        String driverId,
        Instant completedAt
) {
}
