package com.example.ridehailing.web;

import com.example.ridehailing.event.RideRequestedEvent;
import com.example.ridehailing.producer.RideEventProducer;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/rides")
public class RideController {

    private final RideEventProducer rideEventProducer;

    public RideController(RideEventProducer rideEventProducer) {
        this.rideEventProducer = rideEventProducer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> requestRide(@RequestBody RideRequest request) {
        String rideId = UUID.randomUUID().toString();

        rideEventProducer.publishRideRequested(new RideRequestedEvent(
                rideId,
                request.passengerId(),
                request.pickup(),
                request.dropoff(),
                Instant.now()));

        return Map.of(
                "rideId", rideId,
                "status", "ride-requested");
    }

    public record RideRequest(String passengerId, String pickup, String dropoff) {
    }
}
