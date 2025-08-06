package com.example.reactorkafkademo;

import com.example.reactorkafkademo.annotation.AnnotationValidator;
import com.example.reactorkafkademo.core.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class KafkaService {

    private final KafkaReceiver<String, String> receiver;
    private final Executor executor = Executors.newSingleThreadExecutor();
    private Disposable subscribe;

    public KafkaService(@Qualifier("sagaAutomaticBasedReceiver") Optional<KafkaReceiver<String, String>> receiver) {
        this.receiver = receiver.orElse(null);
    }

    @PostConstruct
    public void init() {
        // Receiver logic
        if (this.receiver != null) {
            subscribe = receiver.receive()
                    .flatMap(stringStringReceiverRecord -> {
                        return this.processRecord(stringStringReceiverRecord)
                                .doFinally(signal -> {
                                    log.debug("done>{}", stringStringReceiverRecord.value());
                                    stringStringReceiverRecord.receiverOffset().acknowledge();
                                });
                    }, 1000)
                    .subscribe();
        }
    }

    @PreDestroy
    public void destroy() {
        subscribe.dispose();
        System.out.println("disposed");
    }

    private Mono<Void> processRecord(ConsumerRecord<String, String> _record) {
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

                        } else if (sagaEndpoint instanceof RevertEndpoint revertEndpoint) {

                        }
                    } else {
                        throw new IllegalArgumentException("Endpoint type not supported");
                    }
                }).then();

/*        return Mono.just(_record)
//                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(record -> {
                    System.out.println("before:" + Thread.currentThread().getName() + ":index-" + record.value());
                })
                .flatMap(consumerRecords -> {
                    return databaseClient.sql("SELECT SLEEP(4);")
                            .fetch()
                            .rowsUpdated();
                })
                .doOnNext(record -> {
                    System.out.println("after:" + Thread.currentThread().getName());
                })
                .then();*/
    }
}
