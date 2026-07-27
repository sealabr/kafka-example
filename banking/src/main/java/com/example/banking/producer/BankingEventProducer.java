package com.example.banking.producer;

import com.example.banking.event.AccountDebitedEvent;
import com.example.banking.event.TransferInitiatedEvent;
import com.example.banking.event.TransferSettledEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class BankingEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String transferInitiatedTopic;
    private final String transferSettledTopic;
    private final String accountDebitedTopic;

    public BankingEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${banking.topics.transfer-initiated}") String transferInitiatedTopic,
            @Value("${banking.topics.transfer-settled}") String transferSettledTopic,
            @Value("${banking.topics.account-debited}") String accountDebitedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.transferInitiatedTopic = transferInitiatedTopic;
        this.transferSettledTopic = transferSettledTopic;
        this.accountDebitedTopic = accountDebitedTopic;
    }

    public void publishTransferInitiated(TransferInitiatedEvent event) {
        kafkaTemplate.send(transferInitiatedTopic, event.transferId(), event);
    }

    public void publishTransferSettled(TransferSettledEvent event) {
        kafkaTemplate.send(transferSettledTopic, event.transferId(), event);
    }

    public void publishAccountDebited(AccountDebitedEvent event) {
        kafkaTemplate.send(accountDebitedTopic, event.transferId(), event);
    }
}
