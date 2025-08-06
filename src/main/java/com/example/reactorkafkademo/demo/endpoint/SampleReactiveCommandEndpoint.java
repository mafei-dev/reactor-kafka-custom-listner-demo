package com.example.reactorkafkademo.demo.endpoint;

import com.example.reactorkafkademo.annotation.KafkaEndpoint;
import com.example.reactorkafkademo.annotation.KafkaListener;
import com.example.reactorkafkademo.core.ReactiveCommandEndpoint;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

@KafkaEndpoint
@AllArgsConstructor
public class SampleReactiveCommandEndpoint implements ReactiveCommandEndpoint {
    private final DatabaseClient databaseClient;

    @Override
    @KafkaListener(topic = "SAGA_SAMPLE_REACTIVE_UNDO_PROCESS")
    public Mono<Void> undoProcess(ConsumerRecord<String, String> consumerRecord) {
        return Mono.empty();
    }

    @Override
    @KafkaListener(topic = "SAGA_SAMPLE_REACTIVE_DO_PROCESS")
    public Mono<Void> doProcess(ConsumerRecord<String, String> consumerRecord) {
        return Mono.just(consumerRecord)
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
                .then();
    }
}
