package com.example.reactorkafkademo.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface CommandEndpoint extends SagaEndpoint{
    void undoProcess(ConsumerRecord<String, String> consumerRecord);
}
