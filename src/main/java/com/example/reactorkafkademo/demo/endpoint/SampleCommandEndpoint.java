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
public class SampleCommandEndpoint implements CommandEndpoint {
    private final DatabaseClient databaseClient;

    @Override
    @KafkaListener(topic = "SAGA_SAMPLE_DO_PROCESS")
    public void doProcess(ConsumerRecord<String, String> consumerRecord) {
//        System.out.println("SampleCommandEndpoint.doProcess:"+consumerRecord);
        log.info("SampleCommandEndpoint.doProcess:{}", consumerRecord);
    }

    @Override
    @KafkaListener(topic = "SAGA_SAMPLE_UNDO_PROCESS",partitions = {0})
    public void undoProcess(ConsumerRecord<String, String> consumerRecord) {
//        System.out.println("SampleCommandEndpoint.undoProcess:"+consumerRecord);
        log.info("SampleCommandEndpoint.undoProcess:{}", consumerRecord);
    }
}
