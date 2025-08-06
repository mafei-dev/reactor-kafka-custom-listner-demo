package com.example.reactorkafkademo.annotation;

import com.example.reactorkafkademo.core.*;
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
    private Pair<List<TopicPartition>, List<String>> listListPair;

    public AnnotationValidator(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public Pair<List<TopicPartition>, List<String>> topicPartition() {
        if (listListPair == null) {
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
            List<String> topics = new ArrayList<>(topicDataMap.size());
            List<TopicPartition> partitions = new ArrayList<>(topicDataMap.size());
            topicDataMap.forEach((s, endpointTopicData) -> {
                if (endpointTopicData.getTopicTopicPartition().length == 0) {
                    topics.add(endpointTopicData.getTopicName());
                } else {
                    for (int i : endpointTopicData.getTopicTopicPartition()) {
                        partitions.add(new TopicPartition(endpointTopicData.getTopicName(), i));
                    }
                }
            });
            listListPair = Pair.of(partitions, topics);
        }
        return listListPair;
    }
}
