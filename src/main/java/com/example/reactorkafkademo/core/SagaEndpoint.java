package com.example.reactorkafkademo.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface SagaEndpoint extends Endpoint {
    void doProcess(ConsumerRecord<String, String> consumerRecord);
}
