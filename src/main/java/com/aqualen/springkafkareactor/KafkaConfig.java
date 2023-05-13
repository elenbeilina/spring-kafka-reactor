package com.aqualen.springkafkareactor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Collections;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Bean
    public KafkaReceiver<Integer, String> kafkaReceiver(KafkaProperties properties,
                                                        @Value("${kafka.topic}") String topic) {

        Map<String, Object> consumerProperties = properties.buildConsumerProperties();
        ReceiverOptions<Integer, String> subscription =
                ReceiverOptions.<Integer, String>create(consumerProperties)
                        .subscription(Collections.singleton(topic));
        return new DefaultKafkaReceiver<>(ConsumerFactory.INSTANCE, subscription);
    }

    @Bean
    public KafkaSender<Integer, String> kafkaSender(KafkaProperties properties) {
        Map<String, Object> producerProperties = properties.buildProducerProperties();
        SenderOptions<Integer, String> senderOptions =
                SenderOptions.<Integer, String>create(producerProperties)
                        .maxInFlight(512);
        return KafkaSender.create(senderOptions);
    }
}
