package com.example.reactorkafkademo.config;

import com.example.reactorkafkademo.annotation.AnnotationValidator;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.*;

@Configuration
@AutoConfigureAfter(KafkaAutoConfiguration.class)
@AllArgsConstructor
public class KafkaNonReactiveConfig {
    private final AnnotationValidator annotationValidator;

    @Bean("sagaAutomaticBasedReceiverOptionsNonReactive")
    public ReceiverOptions<String, String> sagaAutomaticBasedReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample_service");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        List<String> topics = annotationValidator.topicPartition().getNonReactiveTopicList();
        if (topics.isEmpty()) {
            return null;
        } else {
            return ReceiverOptions.<String, String>create(props)
                    .commitBatchSize(100)
                    .subscription(topics);
        }
    }

    @Bean("sagaManualBasedReceiverOptionsNonReactive")
    public ReceiverOptions<String, String> sagaManualBasedReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample_service_manual_non_reactive");
//        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        List<TopicPartition> partitions = annotationValidator.topicPartition().getNonReactiveTopicPartitionList();
        if (partitions.isEmpty()) {
            return null;
        } else {
            return ReceiverOptions.<String, String>create(props)
                    .commitBatchSize(20)
                    .commitInterval(Duration.ofMinutes(1))
                    .assignment(partitions);
        }
    }


    @Bean("sagaAutomaticBasedReceiverNonReactive")
    public KafkaReceiver<String, String> sagaAutomaticBasedReceiver(@Qualifier("sagaAutomaticBasedReceiverOptionsNonReactive") Optional<ReceiverOptions<String, String>> receiverOptions) {
        return receiverOptions.map(KafkaReceiver::create).orElse(null);
    }

    @Bean("sagaManualBasedReceiverNonReactive")
    public KafkaReceiver<String, String> sagaManualBasedReceiver(@Qualifier("sagaManualBasedReceiverOptionsNonReactive") Optional<ReceiverOptions<String, String>> receiverOptions) {
        return receiverOptions.map(KafkaReceiver::create).orElse(null);
    }
}
