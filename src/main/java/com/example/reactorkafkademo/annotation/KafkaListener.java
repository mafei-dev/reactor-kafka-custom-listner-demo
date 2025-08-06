package com.example.reactorkafkademo.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaListener {
    String topic();

    int[] partitions() default {};
}
