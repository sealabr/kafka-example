package com.example.ridehailing.consumer;

import com.example.ridehailing.client.DriverMatchingClient;
import com.example.ridehailing.client.NotificationClient;
import com.example.ridehailing.client.PaymentClient;
import com.example.ridehailing.event.DriverAssignedEvent;
import com.example.ridehailing.event.RideCompletedEvent;
import com.example.ridehailing.event.RideRequestedEvent;
import com.example.ridehailing.producer.RideEventProducer;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RideEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RideEventConsumer.class);

    private final DriverMatchingClient driverMatchingClient;
    private final NotificationClient notificationClient;
    private final PaymentClient paymentClient;
    private final RideEventProducer rideEventProducer;

    public RideEventConsumer(
            DriverMatchingClient driverMatchingClient,
            NotificationClient notificationClient,
            PaymentClient paymentClient,
            RideEventProducer rideEventProducer) {
        this.driverMatchingClient = driverMatchingClient;
        this.notificationClient = notificationClient;
        this.paymentClient = paymentClient;
        this.rideEventProducer = rideEventProducer;
    }

    @KafkaListener(topics = "${ride-hailing.topics.ride-requested}", groupId = "${spring.kafka.consumer.group-id}")
    public void onRideRequested(RideRequestedEvent event) {
        log.info("Consumed ride.requested: {}", event);

        String driverId = driverMatchingClient.assignDriver(event);
        rideEventProducer.publishDriverAssigned(new DriverAssignedEvent(
                event.rideId(),
                driverId,
                Instant.now()));
    }

    @KafkaListener(topics = "${ride-hailing.topics.driver-assigned}", groupId = "${spring.kafka.consumer.group-id}")
    public void onDriverAssigned(DriverAssignedEvent event) {
        log.info("Consumed driver.assigned: {}", event);

        notificationClient.notifyDriverAssigned(event);
        rideEventProducer.publishRideCompleted(new RideCompletedEvent(
                event.rideId(),
                event.driverId(),
                Instant.now()));
    }

    @KafkaListener(topics = "${ride-hailing.topics.ride-completed}", groupId = "${spring.kafka.consumer.group-id}")
    public void onRideCompleted(RideCompletedEvent event) {
        log.info("Consumed ride.completed: {}", event);

        paymentClient.chargeRide(event);
    }
}
