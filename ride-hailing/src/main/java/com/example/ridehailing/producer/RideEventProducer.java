package com.example.ridehailing.producer;

import com.example.ridehailing.event.DriverAssignedEvent;
import com.example.ridehailing.event.RideCompletedEvent;
import com.example.ridehailing.event.RideRequestedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RideEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String rideRequestedTopic;
    private final String driverAssignedTopic;
    private final String rideCompletedTopic;

    public RideEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${ride-hailing.topics.ride-requested}") String rideRequestedTopic,
            @Value("${ride-hailing.topics.driver-assigned}") String driverAssignedTopic,
            @Value("${ride-hailing.topics.ride-completed}") String rideCompletedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.rideRequestedTopic = rideRequestedTopic;
        this.driverAssignedTopic = driverAssignedTopic;
        this.rideCompletedTopic = rideCompletedTopic;
    }

    public void publishRideRequested(RideRequestedEvent event) {
        kafkaTemplate.send(rideRequestedTopic, event.rideId(), event);
    }

    public void publishDriverAssigned(DriverAssignedEvent event) {
        kafkaTemplate.send(driverAssignedTopic, event.rideId(), event);
    }

    public void publishRideCompleted(RideCompletedEvent event) {
        kafkaTemplate.send(rideCompletedTopic, event.rideId(), event);
    }
}
