package com.example.reactorkafkademo.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;

public interface SagaReactiveEndpoint extends Endpoint {
    Mono<Void> doProcess(ConsumerRecord<String, String> consumerRecord);
}
