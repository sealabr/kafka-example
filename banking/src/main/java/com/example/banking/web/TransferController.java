package com.example.banking.web;

import com.example.banking.event.AccountDebitedEvent;
import com.example.banking.event.TransferInitiatedEvent;
import com.example.banking.event.TransferSettledEvent;
import com.example.banking.producer.BankingEventProducer;
import java.math.BigDecimal;
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
@RequestMapping("/api/transfers")
public class TransferController {

    private final BankingEventProducer bankingEventProducer;

    public TransferController(BankingEventProducer bankingEventProducer) {
        this.bankingEventProducer = bankingEventProducer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> initiateTransfer(@RequestBody TransferRequest request) {
        String transferId = UUID.randomUUID().toString();

        bankingEventProducer.publishTransferInitiated(new TransferInitiatedEvent(
                transferId,
                request.fromAccount(),
                request.toAccount(),
                request.amount(),
                Instant.now()));

        bankingEventProducer.publishAccountDebited(new AccountDebitedEvent(
                transferId,
                request.fromAccount(),
                request.amount(),
                Instant.now()));

        bankingEventProducer.publishTransferSettled(new TransferSettledEvent(
                transferId,
                Instant.now()));

        return Map.of(
                "transferId", transferId,
                "status", "events-published");
    }

    public record TransferRequest(String fromAccount, String toAccount, BigDecimal amount) {
    }
}
