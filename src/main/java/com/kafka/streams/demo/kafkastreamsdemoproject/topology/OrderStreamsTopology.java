package com.kafka.streams.demo.kafkastreamsdemoproject.topology;

import com.kafka.streams.demo.kafkastreamsdemoproject.domain.Order;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerMeatType;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerStore;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerStorePerMeatType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderStreamsTopology {
    public static final String ORDERS = "orders";
    public static final String ORDERS_COUNT_PER_STORE = "orders_per_store";

    public static final String ORDERS_COUNT_PER_MEAT_TYPE = "orders_per_meat_type";

    public static final String REVENUE_TOTAL_PER_STORE = "revenue_per_store";

    public static final String REVENUE_TOTAL_PER_MEAT_TYPE = "revenue_per_meat_type";
    public static final String REVENUE_TOTAL_PER_MEAT_TYPE_PER_STORE = "revenue_per_meat_type_per_store";

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        orderTopology(streamsBuilder);

    }

    public void orderTopology(StreamsBuilder streamsBuilder) {

        var ordersStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)));

        ordersStream.print(Printed.<String, Order>toSysOut().withLabel("ordersStream"));

        countAllOrdersPerStore(ordersStream, ORDERS_COUNT_PER_STORE);
        countAllOrdersPerMeatTypes(ordersStream, ORDERS_COUNT_PER_MEAT_TYPE);
        aggregateRevenuePerStore(ordersStream, REVENUE_TOTAL_PER_STORE);
        aggregateRevenuePerMeatType(ordersStream, REVENUE_TOTAL_PER_MEAT_TYPE);
        aggregateRevenuePerMeatTypePerStore(ordersStream, REVENUE_TOTAL_PER_MEAT_TYPE_PER_STORE);
    }

    private void countAllOrdersPerStore(KStream<String, Order> ordersStream, String ordersCountPerStore) {

        var ordersPerStoreCount = ordersStream
                .selectKey((key, value) -> value.getStoreCode())
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .count(Named.as(ordersCountPerStore), Materialized.as(ordersCountPerStore));

        ordersPerStoreCount.toStream().print(Printed.<String, Long>toSysOut().withLabel(ordersCountPerStore));
    }

    private void countAllOrdersPerMeatTypes(KStream<String, Order> ordersStream, String storeName) {

        var ordersPerMeatTypeCount = ordersStream
                .map((key, value) -> KeyValue.pair(value.getMeatType().name(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .count(Named.as(ORDERS_COUNT_PER_MEAT_TYPE), Materialized.as(storeName));

        ordersPerMeatTypeCount.toStream().print(Printed.<String, Long>toSysOut().withLabel("ORDERS COUNT PER MEAT TYPE"));

    }

    private void aggregateRevenuePerStore(KStream<String, Order> ordersStream, String revenuePerStoreStoreName) {

        Initializer<TotalRevenuePerStore> totalRevenuePerStoreInitializer = TotalRevenuePerStore::new;
        Aggregator<String, Order, TotalRevenuePerStore> totalRevenuePerStoreAggregator = (key, order, aggregate) -> {

            return aggregate.incrementTotalRevenuePerStore(key, order);
        };

        var revenuePerStoreTable = ordersStream
                .map((key, value) -> KeyValue.pair(value.getStoreCode(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .aggregate(totalRevenuePerStoreInitializer,
                        totalRevenuePerStoreAggregator,
                        Materialized.
                                <String, TotalRevenuePerStore, KeyValueStore<Bytes, byte[]>>
                                as(revenuePerStoreStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenuePerStore.class)));

        revenuePerStoreTable.toStream()
                .print(Printed.<String, TotalRevenuePerStore>toSysOut()
                        .withLabel(revenuePerStoreStoreName));
    }

    private void aggregateRevenuePerMeatType(KStream<String, Order> ordersStream, String revenuePerMeatTypeStoreName) {

        Initializer<TotalRevenuePerMeatType> totalRevenuePerMeatTypeInitializer = TotalRevenuePerMeatType::new;
        Aggregator<String, Order, TotalRevenuePerMeatType> totalRevenuePerMeatTypeAggregator = (key, order, aggregate) -> {
            return aggregate.calculateRunningRevenue(key, order);
        };

        var revenuePerMeatTypeTable = ordersStream
                .map((key, value) -> KeyValue.pair(value.getMeatType().name(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .aggregate(totalRevenuePerMeatTypeInitializer,
                        totalRevenuePerMeatTypeAggregator,
                        Materialized.
                                <String, TotalRevenuePerMeatType, KeyValueStore<Bytes, byte[]>>
                                as(revenuePerMeatTypeStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenuePerMeatType.class)));

        revenuePerMeatTypeTable.toStream()
                .print(Printed.<String, TotalRevenuePerMeatType>toSysOut()
                        .withLabel(revenuePerMeatTypeStoreName));

    }

    private void aggregateRevenuePerMeatTypePerStore(KStream<String, Order> ordersStream, String revenuePerStorePerMeatTypePerName) {

        Initializer<TotalRevenuePerStorePerMeatType> totalRevenuePerStorePerMeatTypeInitializer = TotalRevenuePerStorePerMeatType::new;
        Aggregator<String, Order, TotalRevenuePerStorePerMeatType> totalRevenuePerMeatTypeAggregator = (key, order, aggregate) -> {
            return aggregate.incrementTotalRevenuePerStore(order);
        };

        var revenuePerStorePerMeatTypeTable = ordersStream
                .selectKey((key, order) -> order.getStoreCode().concat("-" + order.getMeatType().name()))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .aggregate(totalRevenuePerStorePerMeatTypeInitializer,
                        totalRevenuePerMeatTypeAggregator,
                        Materialized.<String, TotalRevenuePerStorePerMeatType, KeyValueStore<Bytes, byte[]>>
                                        as(revenuePerStorePerMeatTypePerName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenuePerStorePerMeatType.class)));

        revenuePerStorePerMeatTypeTable.toStream()
                .print(Printed.<String, TotalRevenuePerStorePerMeatType>toSysOut()
                        .withLabel(revenuePerStorePerMeatTypePerName));
    }

    //aggregation on orders
    //count on orders
    //reduce on orders for revenue
}
