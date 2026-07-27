package com.example.banking.consumer;

import com.example.banking.event.AccountDebitedEvent;
import com.example.banking.event.TransferInitiatedEvent;
import com.example.banking.event.TransferSettledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BankingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(BankingEventConsumer.class);

    @KafkaListener(topics = "${banking.topics.transfer-initiated}", groupId = "${spring.kafka.consumer.group-id}")
    public void onTransferInitiated(TransferInitiatedEvent event) {
        log.info("Consumed transfer.initiated: {}", event);
    }

    @KafkaListener(topics = "${banking.topics.transfer-settled}", groupId = "${spring.kafka.consumer.group-id}")
    public void onTransferSettled(TransferSettledEvent event) {
        log.info("Consumed transfer.settled: {}", event);
    }

    @KafkaListener(topics = "${banking.topics.account-debited}", groupId = "${spring.kafka.consumer.group-id}")
    public void onAccountDebited(AccountDebitedEvent event) {
        log.info("Consumed account.debited: {}", event);
    }
}
