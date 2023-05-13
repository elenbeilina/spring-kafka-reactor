package com.aqualen.springkafkareactor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Service
@RequiredArgsConstructor
public class Receiver {

    private final KafkaReceiver<Integer, String> kafkaReceiver;

    public Flux<String> receive() {
        return kafkaReceiver.receive()
                .checkpoint("Started receiving messages.")
                .log()
                .doOnNext(receiverRecord -> receiverRecord.receiverOffset().acknowledge())
                .map(ReceiverRecord::value)
                .checkpoint("Messages are received.")
                .retryWhen(Retry.backoff(3, Duration.of(10L, ChronoUnit.SECONDS)));
    }
}

