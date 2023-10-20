package com.kafka.streams.demo.kafkastreamsdemoproject.config;

import com.kafka.streams.demo.kafkastreamsdemoproject.exceptionhandler.StreamsProcessorCustomErrorHandler;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderStreamsTopology.ORDERS;

@Configuration
public class KafkaStreamsConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;

    @Value("${server.port}")
    private int port;

    @Value("${spring.application.name}")
    private String applicationName;


    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public org.springframework.kafka.config.KafkaStreamsConfiguration kafkaStreamsConfiguration(){

        var streamProperties = kafkaProperties.buildStreamsProperties();
        try {
            streamProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, InetAddress.getLocalHost().getHostAddress()+":"+port);
            streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, String.format("%s%s",applicationName,port));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        return new org.springframework.kafka.config.KafkaStreamsConfiguration(streamProperties);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());
        };
    }


    @Bean
    public NewTopic createTopic(){
        return TopicBuilder.name(ORDERS)
                .replicas(1)
                .partitions(2)
                .build();
    }

}
