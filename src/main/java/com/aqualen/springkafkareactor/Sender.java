package com.aqualen.springkafkareactor;

import com.github.javafaker.Faker;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class Sender {

    private final KafkaSender<Integer, String> kafkaSender;
    @Value("${kafka.topic}")
    private String topic;

    public void sendToKafka(Integer count) {
        kafkaSender.send(generateMessages(count))
                .doOnError(error -> log.error("Send failed, terminating.", error))
                .doOnNext(senderResult -> System.out.printf("Message #%d response: %s.%n",
                        senderResult.correlationMetadata(), senderResult.recordMetadata()))
                .retryWhen(Retry.backoff(3, Duration.of(10L, ChronoUnit.SECONDS)))
                .doOnCancel(this::closeSender)
                .subscribe();
    }

    private Flux<SenderRecord<Integer, String, Integer>> generateMessages(Integer count) {
        return Flux.range(1, count)
                .map(i -> SenderRecord.create(
                        new ProducerRecord<>(
                                topic, i, new Faker().beer().name()), i)
                );
    }

    @PreDestroy
    private void closeSender() {
        kafkaSender.close();
    }
}
