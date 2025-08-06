package com.example.reactorkafkademo;

import com.example.reactorkafkademo.annotation.AnnotationValidator;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Configuration
@AutoConfigureAfter(KafkaAutoConfiguration.class)
@AllArgsConstructor
public class KafkaConfig {
    private final AnnotationValidator annotationValidator;

    @Bean("sagaAutomaticBasedReceiverOptions")
    public ReceiverOptions<String, String> sagaAutomaticBasedReceiverOptions(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample_service");
        List<String> topics = annotationValidator.topicPartition().getSecond();
        if (topics.isEmpty()) {
            return null;
        } else {
            return ReceiverOptions.<String, String>create(props)
                    .commitBatchSize(20)
                    .subscription(topics);
        }
    }

    @Bean("sagaManualBasedReceiverOptions")
    public ReceiverOptions<String, String> sagaManualBasedReceiverOptions(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample_service");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "sample_service");
        List<TopicPartition> partitions = annotationValidator.topicPartition().getFirst();
        if (partitions.isEmpty()) {
            return null;
        } else {
            return ReceiverOptions.<String, String>create(props)
                    .commitBatchSize(20)
                    .assignment(partitions);
        }
    }


    @Bean("sagaAutomaticBasedReceiver")
    public KafkaReceiver<String, String> sagaAutomaticBasedReceiver(@Qualifier("sagaAutomaticBasedReceiverOptions") Optional<ReceiverOptions<String, String>> receiverOptions) {
        return receiverOptions.map(KafkaReceiver::create).orElse(null);
    }

    @Bean("sagaManualBasedReceiver")
    public KafkaReceiver<String, String> sagaManualBasedReceiver(@Qualifier("sagaManualBasedReceiverOptions") Optional<ReceiverOptions<String, String>> receiverOptions) {
        return receiverOptions.map(KafkaReceiver::create).orElse(null);
    }


    @Bean
    public SenderOptions<String, String> senderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return SenderOptions.create(props);
    }

    @Bean
    public KafkaSender<String, String> kafkaSender(SenderOptions<String, String> senderOptions) {
        return KafkaSender.create(senderOptions);
    }
}
