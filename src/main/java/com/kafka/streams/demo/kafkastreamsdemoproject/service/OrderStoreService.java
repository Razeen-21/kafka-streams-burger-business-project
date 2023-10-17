package com.kafka.streams.demo.kafkastreamsdemoproject.service;

import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderCountDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerMeatType;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerStore;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerStorePerMeatType;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class OrderStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;


    public OrderStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public ReadOnlyKeyValueStore<String, Long> ordersStore(String storeName) {
        return Objects.requireNonNull(
                        streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }

    public ReadOnlyKeyValueStore<String, TotalRevenuePerStore> ordersRevenuePerStoreStore(String storeName) {
        return Objects.requireNonNull(
                        streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }

    public ReadOnlyKeyValueStore<String, TotalRevenuePerMeatType> ordersRevenuePerMeatTypeStore(String storeName) {
        return Objects.requireNonNull(
                        streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }

    public ReadOnlyKeyValueStore<String, TotalRevenuePerStorePerMeatType> ordersRevenuePerStorePerMeatTypeStore(String storeName) {
        return Objects.requireNonNull(
                        streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }

}
