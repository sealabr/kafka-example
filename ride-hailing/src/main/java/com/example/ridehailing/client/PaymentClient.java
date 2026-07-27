package com.example.ridehailing.client;

import com.example.ridehailing.event.RideCompletedEvent;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class PaymentClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentClient.class);

    private final RestClient restClient;

    public PaymentClient(RestClient paymentRestClient) {
        this.restClient = paymentRestClient;
    }

    public void chargeRide(RideCompletedEvent event) {
        restClient.post()
                .uri("/api/payments/charge")
                .body(Map.of(
                        "rideId", event.rideId(),
                        "driverId", event.driverId(),
                        "completedAt", event.completedAt().toString()))
                .retrieve()
                .toBodilessEntity();

        log.info("Payment API charged rideId={}", event.rideId());
    }
}
