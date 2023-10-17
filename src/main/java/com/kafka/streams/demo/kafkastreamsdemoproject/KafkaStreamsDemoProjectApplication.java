package com.kafka.streams.demo.kafkastreamsdemoproject;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
//add this annotation to make application behave like a kafka streams application
@EnableKafkaStreams
public class KafkaStreamsDemoProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsDemoProjectApplication.class, args);
	}

}
