package com.example.reactorkafkademo.annotation;

import com.example.reactorkafkademo.core.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationContext;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class AnnotationValidator {
    public final static Map<String, EndpointTopicData> topicDataMap = new HashMap<>();
    private final ApplicationContext applicationContext;
    private TopicMetadata topicMetadata;

    public AnnotationValidator(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public TopicMetadata topicPartition() {
        if (topicMetadata == null) {
            Map<String, Endpoint> endpoints = applicationContext.getBeansOfType(Endpoint.class);
            endpoints.forEach((s, endpoint) -> {
                if (endpoint instanceof SagaEndpoint sagaEndpoint) {
                    if (sagaEndpoint instanceof CommandEndpoint commandEndpoint) {
                        KafkaEndpoint kafkaEndpoint = commandEndpoint.getClass().getAnnotation(KafkaEndpoint.class);
                        if (topicDataMap.containsKey(kafkaEndpoint.value())) {
                            throw new RuntimeException("SagaEndpoint bean already exists");
                        } else {

                            try {
                                {
                                    EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.NON_REACTIVE);
                                    endpointTopicData.setEndpoint(commandEndpoint);
                                    Method doProcess = commandEndpoint.getClass().getMethod("doProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = doProcess.getDeclaredAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.COMMAND_DO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                                {
                                    EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.NON_REACTIVE);
                                    endpointTopicData.setEndpoint(commandEndpoint);
                                    Method undoProcess = commandEndpoint.getClass().getMethod("undoProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = undoProcess.getDeclaredAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.COMMAND_UNDO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (sagaEndpoint instanceof QueryEndpoint queryEndpoint) {
                        KafkaEndpoint kafkaEndpoint = queryEndpoint.getClass().getAnnotation(KafkaEndpoint.class);
                        if (topicDataMap.containsKey(kafkaEndpoint.value())) {
                            throw new RuntimeException("SagaEndpoint bean already exists");
                        } else {
                            EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.NON_REACTIVE);
                            endpointTopicData.setEndpoint(queryEndpoint);
                            try {
                                {
                                    Method doProcess = queryEndpoint.getClass().getMethod("doProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = doProcess.getAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.QUERY_DO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (sagaEndpoint instanceof RevertEndpoint revertEndpoint) {
                        KafkaEndpoint kafkaEndpoint = revertEndpoint.getClass().getAnnotation(KafkaEndpoint.class);
                        if (topicDataMap.containsKey(kafkaEndpoint.value())) {
                            throw new RuntimeException("SagaEndpoint bean already exists");
                        } else {
                            EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.NON_REACTIVE);
                            endpointTopicData.setEndpoint(revertEndpoint);
                            try {
                                {
                                    Method doProcess = revertEndpoint.getClass().getMethod("doProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = doProcess.getAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.REVERT_DO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                } else if (endpoint instanceof SagaReactiveEndpoint sagaReactiveEndpoint) {
                    if (sagaReactiveEndpoint instanceof ReactiveCommandEndpoint commandEndpoint) {
                        KafkaEndpoint kafkaEndpoint = commandEndpoint.getClass().getAnnotation(KafkaEndpoint.class);
                        if (topicDataMap.containsKey(kafkaEndpoint.value())) {
                            throw new RuntimeException("SagaEndpoint bean already exists");
                        } else {

                            try {
                                {
                                    EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.REACTIVE);
                                    endpointTopicData.setEndpoint(commandEndpoint);
                                    Method doProcess = commandEndpoint.getClass().getMethod("doProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = doProcess.getDeclaredAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.COMMAND_DO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                                {
                                    EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.REACTIVE);
                                    endpointTopicData.setEndpoint(commandEndpoint);
                                    Method undoProcess = commandEndpoint.getClass().getMethod("undoProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = undoProcess.getDeclaredAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.COMMAND_UNDO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (sagaReactiveEndpoint instanceof ReactiveQueryEndpoint queryEndpoint) {
                        KafkaEndpoint kafkaEndpoint = queryEndpoint.getClass().getAnnotation(KafkaEndpoint.class);
                        if (topicDataMap.containsKey(kafkaEndpoint.value())) {
                            throw new RuntimeException("SagaEndpoint bean already exists");
                        } else {
                            EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.REACTIVE);
                            endpointTopicData.setEndpoint(queryEndpoint);
                            try {
                                {
                                    Method doProcess = queryEndpoint.getClass().getMethod("doProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = doProcess.getAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.QUERY_DO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (sagaReactiveEndpoint instanceof ReactiveRevertEndpoint revertEndpoint) {
                        KafkaEndpoint kafkaEndpoint = revertEndpoint.getClass().getAnnotation(KafkaEndpoint.class);
                        if (topicDataMap.containsKey(kafkaEndpoint.value())) {
                            throw new RuntimeException("SagaEndpoint bean already exists");
                        } else {
                            EndpointTopicData endpointTopicData = new EndpointTopicData(ImplType.REACTIVE);
                            endpointTopicData.setEndpoint(revertEndpoint);
                            try {
                                {
                                    Method doProcess = revertEndpoint.getClass().getMethod("doProcess", ConsumerRecord.class);
                                    KafkaListener kafkaListener = doProcess.getAnnotation(KafkaListener.class);
                                    endpointTopicData.setTopicName(kafkaListener.topic());
                                    endpointTopicData.setTopicTopicPartition(kafkaListener.partitions());
                                    endpointTopicData.setEndpointMethodType(EndpointMethodType.REVERT_DO_PROCESS);
                                    topicDataMap.put(kafkaListener.topic(), endpointTopicData);
                                }
                            } catch (NoSuchMethodException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }

            });
            List<String> reactiveTopics = new ArrayList<>();
            List<TopicPartition> reactivePartitions = new ArrayList<>();
            List<String> nonReactiveTopics = new ArrayList<>();
            List<TopicPartition> nonReactivePartitions = new ArrayList<>();

            topicDataMap.forEach((s, endpointTopicData) -> {
                if (endpointTopicData.getTopicTopicPartition().length == 0) {
                    if (endpointTopicData.getImplType().equals(ImplType.REACTIVE)) {
                        reactiveTopics.add(endpointTopicData.getTopicName());
                    } else if (endpointTopicData.getImplType().equals(ImplType.NON_REACTIVE)) {
                        nonReactiveTopics.add(endpointTopicData.getTopicName());
                    }
                } else {
                    if (endpointTopicData.getImplType().equals(ImplType.REACTIVE)) {
                        for (int i : endpointTopicData.getTopicTopicPartition()) {
                            reactivePartitions.add(new TopicPartition(endpointTopicData.getTopicName(), i));
                        }
                    } else if (endpointTopicData.getImplType().equals(ImplType.NON_REACTIVE)) {
                        for (int i : endpointTopicData.getTopicTopicPartition()) {
                            nonReactivePartitions.add(new TopicPartition(endpointTopicData.getTopicName(), i));
                        }
                    }
                }
            });
            topicMetadata = new TopicMetadata();
            topicMetadata.setReactiveTopicList(reactiveTopics);
            topicMetadata.setNonReactiveTopicList(nonReactiveTopics);
            topicMetadata.setReactiveTopicPartitionList(reactivePartitions);
            topicMetadata.setNonReactiveTopicPartitionList(nonReactivePartitions);
        }
        return topicMetadata;
    }

    @Setter
    @Getter
    public static class TopicMetadata {
        private List<TopicPartition> reactiveTopicPartitionList;
        private List<TopicPartition> nonReactiveTopicPartitionList;
        private List<String> reactiveTopicList;
        private List<String> nonReactiveTopicList;
    }
}

