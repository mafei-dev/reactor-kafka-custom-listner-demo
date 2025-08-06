package com.example.reactorkafkademo.controller;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@RestController
@RequestMapping("/sample")
@AllArgsConstructor
public class SampleController {

    private final KafkaSender<String, String> sender;


    @GetMapping("/{q}")
    public void invoke(@PathVariable Integer q) {
        // Sender logic (send a message every 5 seconds)
        Flux.range(0, q)
                .map(i -> SenderRecord.create(new ProducerRecord<>("SAGA_SAMPLE_UNDO_PROCESS", "key", "Hello Kafka " + i), null))
                .as(sender::send)
                .subscribe(result -> System.out.println("Sent: " + result.recordMetadata().offset()));
    }
}
