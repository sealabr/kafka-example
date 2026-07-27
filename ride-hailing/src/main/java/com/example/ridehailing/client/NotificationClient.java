package com.example.ridehailing.client;

import com.example.ridehailing.event.DriverAssignedEvent;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class NotificationClient {

    private static final Logger log = LoggerFactory.getLogger(NotificationClient.class);

    private final RestClient restClient;

    public NotificationClient(RestClient notificationRestClient) {
        this.restClient = notificationRestClient;
    }

    public void notifyDriverAssigned(DriverAssignedEvent event) {
        restClient.post()
                .uri("/api/notifications/driver-assigned")
                .body(Map.of(
                        "rideId", event.rideId(),
                        "driverId", event.driverId(),
                        "assignedAt", event.assignedAt().toString()))
                .retrieve()
                .toBodilessEntity();

        log.info("Notification API notified driver assigned for rideId={}", event.rideId());
    }
}
