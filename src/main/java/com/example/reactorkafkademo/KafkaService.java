package com.example.reactorkafkademo;

import com.example.reactorkafkademo.annotation.AnnotationValidator;
import com.example.reactorkafkademo.core.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class KafkaService {

    private final KafkaReceiver<String, String> nonReactiveReceiver;
    private final KafkaReceiver<String, String> reactiveReceiver;
    private final KafkaReceiver<String, String> manualNonReactiveReceiver;
    private final KafkaReceiver<String, String> manualReactiveReceiver;

    private final Executor executor = Executors.newSingleThreadExecutor();

    public KafkaService(
            @Qualifier("sagaAutomaticBasedReceiverNonReactive") Optional<KafkaReceiver<String, String>> nonReactiveReceiver,
            @Qualifier("sagaAutomaticBasedReceiverReactive") Optional<KafkaReceiver<String, String>> reactiveReceiver,
            @Qualifier("sagaManualBasedReceiverNonReactive") Optional<KafkaReceiver<String, String>> manualNonReactiveReceiver,
            @Qualifier("sagaManualBasedReceiverReactive") Optional<KafkaReceiver<String, String>> manualReactiveReceiver

    ) {
        this.nonReactiveReceiver = nonReactiveReceiver.orElse(null);
        this.reactiveReceiver = reactiveReceiver.orElse(null);
        this.manualNonReactiveReceiver = manualNonReactiveReceiver.orElse(null);
        this.manualReactiveReceiver = manualReactiveReceiver.orElse(null);
    }

    @PostConstruct
    public void init() {
        // Receiver logic
        if (this.nonReactiveReceiver != null) {
            nonReactiveReceiver.receive()
                    .flatMap(stringStringReceiverRecord -> {
                        return this.processNonReactiveRecord(stringStringReceiverRecord)
                                .doFinally(signal -> {
                                    log.debug("done>{}", stringStringReceiverRecord.value());
                                    stringStringReceiverRecord.receiverOffset().acknowledge();
                                });
                    }, 1000)
                    .subscribe();
        }
        if (this.manualNonReactiveReceiver != null) {
            manualNonReactiveReceiver.receive()
                    .flatMap(stringStringReceiverRecord -> {
                        return this.processNonReactiveRecord(stringStringReceiverRecord)
                                .doFinally(signal -> {
                                    log.debug("done>{}", stringStringReceiverRecord.value());
                                    stringStringReceiverRecord.receiverOffset().acknowledge();
                                });
                    }, 1000)
                    .subscribe();
        }

        if (this.reactiveReceiver != null) {
            this.nonReactive(reactiveReceiver);
        }
        if (this.manualReactiveReceiver != null) {
            this.nonReactive(manualReactiveReceiver);
        }
    }

    private void nonReactive(KafkaReceiver<String, String> manualReactiveReceiver) {
        manualReactiveReceiver.receive()
                .flatMap(stringStringReceiverRecord -> {
                    System.out.println("stringStringReceiverRecord = " + stringStringReceiverRecord);
                    return this.processReactiveRecord(stringStringReceiverRecord)
                            .onErrorResume(throwable -> {
                                throwable.printStackTrace();
                                return Mono.empty();
                            })
                            .doFinally(signal -> {
                                log.debug("done>{}", stringStringReceiverRecord.value());
                                stringStringReceiverRecord.receiverOffset().acknowledge();
                            });
                }, 1000)
                .subscribe();
    }

    @PreDestroy
    public void destroy() {
    }

    private Mono<Void> processReactiveRecord(ConsumerRecord<String, String> _record) {
        EndpointTopicData endpointTopicData = AnnotationValidator.topicDataMap.get(_record.topic());
        Endpoint endpoint = endpointTopicData.getEndpoint();
        if (endpoint instanceof SagaReactiveEndpoint sagaEndpoint) {
            if (sagaEndpoint instanceof ReactiveCommandEndpoint commandEndpoint) {
                if (endpointTopicData.getEndpointMethodType().equals(EndpointMethodType.COMMAND_DO_PROCESS)) {
                    return commandEndpoint.doProcess(_record);
                } else if (endpointTopicData.getEndpointMethodType().equals(EndpointMethodType.COMMAND_UNDO_PROCESS)) {
                    return commandEndpoint.undoProcess(_record);
                } else {
                    return Mono.empty();
                }
            } else if (sagaEndpoint instanceof ReactiveQueryEndpoint queryEndpoint) {
                return queryEndpoint.doProcess(_record);
            } else if (sagaEndpoint instanceof ReactiveRevertEndpoint revertEndpoint) {
                return revertEndpoint.doProcess(_record);
            } else {
                return Mono.empty();
            }
        } else {
            throw new IllegalArgumentException("Endpoint type not supported");
        }
    }

    private Mono<Void> processNonReactiveRecord(ConsumerRecord<String, String> _record) {
        return Mono
                .fromRunnable(() -> {
                    EndpointTopicData endpointTopicData = AnnotationValidator.topicDataMap.get(_record.topic());
                    Endpoint endpoint = endpointTopicData.getEndpoint();
                    if (endpoint instanceof SagaEndpoint sagaEndpoint) {
                        if (sagaEndpoint instanceof CommandEndpoint commandEndpoint) {
                            if (endpointTopicData.getEndpointMethodType().equals(EndpointMethodType.COMMAND_DO_PROCESS)) {
                                commandEndpoint.doProcess(_record);
                            } else if (endpointTopicData.getEndpointMethodType().equals(EndpointMethodType.COMMAND_UNDO_PROCESS)) {
                                commandEndpoint.undoProcess(_record);
                            }
                        } else if (sagaEndpoint instanceof QueryEndpoint queryEndpoint) {
                            queryEndpoint.doProcess(_record);
                        } else if (sagaEndpoint instanceof RevertEndpoint revertEndpoint) {
                            revertEndpoint.doProcess(_record);
                        }
                    } else {
                        throw new IllegalArgumentException("Endpoint type not supported");
                    }
                })
                .then();
    }

}
