package com.example.reactorkafkademo.demo.endpoint;

import com.example.reactorkafkademo.annotation.KafkaEndpoint;
import com.example.reactorkafkademo.annotation.KafkaListener;
import com.example.reactorkafkademo.core.CommandEndpoint;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.r2dbc.core.DatabaseClient;

@Slf4j
@KafkaEndpoint
@AllArgsConstructor
public class SampleCommandEndpoint2 implements CommandEndpoint {
    private final DatabaseClient databaseClient;

    @Override
    @KafkaListener(topic = "SAGA_SAMPLE_1_DO_PROCESS")
    public void doProcess(ConsumerRecord<String, String> consumerRecord) {
//        System.out.println("SampleCommandEndpoint.doProcess:"+consumerRecord);
        log.info("SampleCommandEndpoint2.doProcess:key{},partition:{}", consumerRecord.key(), consumerRecord.partition());
    }

    @Override
    @KafkaListener(topic = "SAGA_SAMPLE_1_UNDO_PROCESS", partitions = {0})
    public void undoProcess(ConsumerRecord<String, String> consumerRecord) {
//        System.out.println("SampleCommandEndpoint.undoProcess:"+consumerRecord);
        log.info("SampleCommandEndpoint2.undoProcess:{}", consumerRecord);
    }
}
