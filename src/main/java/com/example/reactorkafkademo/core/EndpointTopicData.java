package com.example.reactorkafkademo.core;

import lombok.Data;

@Data
public class EndpointTopicData {
    private final ImplType implType;
    private Endpoint endpoint;
    private String topicName;
    private int[] topicTopicPartition;
    private EndpointMethodType endpointMethodType;
}
