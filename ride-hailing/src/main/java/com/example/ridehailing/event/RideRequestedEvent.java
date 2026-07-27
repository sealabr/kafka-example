package com.example.ridehailing.event;

import java.time.Instant;

public record RideRequestedEvent(
        String rideId,
        String passengerId,
        String pickup,
        String dropoff,
        Instant requestedAt
) {
}
