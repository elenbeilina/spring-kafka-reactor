package com.aqualen.springkafkareactor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;

@Service
@RequiredArgsConstructor
public class Resiver {

    private final KafkaReceiver<Integer, String> kafkaReceiver;

    public 
}
