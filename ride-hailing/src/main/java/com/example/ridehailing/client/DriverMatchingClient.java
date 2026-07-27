package com.example.ridehailing.client;

import com.example.ridehailing.event.RideRequestedEvent;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class DriverMatchingClient {

    private static final Logger log = LoggerFactory.getLogger(DriverMatchingClient.class);

    private final RestClient restClient;

    public DriverMatchingClient(RestClient driverMatchingRestClient) {
        this.restClient = driverMatchingRestClient;
    }

    public String assignDriver(RideRequestedEvent ride) {
        String driverId = "driver-" + UUID.randomUUID().toString().substring(0, 8);

        restClient.post()
                .uri("/api/matching/assign")
                .body(Map.of(
                        "rideId", ride.rideId(),
                        "passengerId", ride.passengerId(),
                        "pickup", ride.pickup(),
                        "dropoff", ride.dropoff(),
                        "driverId", driverId))
                .retrieve()
                .toBodilessEntity();

        log.info("Driver matching API assigned driverId={} for rideId={}", driverId, ride.rideId());
        return driverId;
    }
}
