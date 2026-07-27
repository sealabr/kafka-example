package com.example.ridehailing.event;

import java.time.Instant;

public record DriverAssignedEvent(
        String rideId,
        String driverId,
        Instant assignedAt
) {
}
