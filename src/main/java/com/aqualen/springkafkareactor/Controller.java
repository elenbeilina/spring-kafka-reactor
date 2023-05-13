package com.aqualen.springkafkareactor;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class Controller {

    private final Sender sender;
    private final Receiver receiver;

    @PostMapping("{count}")
    void sendEvents(@PathVariable Integer count) {
        sender.sendToKafka(count);
    }

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<String> getEventsFlux() {
        return receiver.receive();
    }
}
