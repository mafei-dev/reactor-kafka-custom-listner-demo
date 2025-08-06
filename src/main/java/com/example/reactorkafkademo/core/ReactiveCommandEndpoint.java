package com.example.reactorkafkademo.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;

public interface ReactiveCommandEndpoint extends SagaReactiveEndpoint {
    Mono<Void> undoProcess(ConsumerRecord<String, String> consumerRecord);
}
