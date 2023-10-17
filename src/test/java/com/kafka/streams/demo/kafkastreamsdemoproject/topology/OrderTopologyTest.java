package com.kafka.streams.demo.kafkastreamsdemoproject.topology;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.MeatType;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.Order;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerMeatType;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerStore;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.TotalRevenuePerStorePerMeatType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.KafkaStreamsTopology.ORDERS;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.KafkaStreamsTopology.ORDERS_COUNT_PER_MEAT_TYPE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.KafkaStreamsTopology.ORDERS_COUNT_PER_STORE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.KafkaStreamsTopology.REVENUE_TOTAL_PER_MEAT_TYPE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.KafkaStreamsTopology.REVENUE_TOTAL_PER_MEAT_TYPE_PER_STORE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.KafkaStreamsTopology.REVENUE_TOTAL_PER_STORE;
import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest
public class OrderTopologyTest {


    TopologyTestDriver topologyTestDriver = null;

    TestInputTopic<String, String> inputTopic = null;

    static ObjectMapper objectMapper = new ObjectMapper();
    KafkaStreamsTopology kafkaStreamsTopology = new KafkaStreamsTopology();
    StreamsBuilder streamsBuilder = null;

    private static String generateBarcode() {
        var barCode = UUID.randomUUID();
        var removeUnderscores = barCode.toString().replace("_", "0");
        var removeDashes = removeUnderscores.replace("-", "0");
        return removeDashes.substring(0, 16);
    }

    @BeforeEach
    void setUp() {
        streamsBuilder = new StreamsBuilder();
        kafkaStreamsTopology.process(streamsBuilder);
        var topology = streamsBuilder.build();
        topologyTestDriver = new TopologyTestDriver(topology);
        inputTopic = topologyTestDriver.createInputTopic(ORDERS, Serdes.String().serializer(), Serdes.String().serializer());
    }

    @AfterEach
    void breakDown() {
        topologyTestDriver.close();
    }

    @Test
    void ordersCountPerStoreTest() {
        //given
        try {
            inputTopic.pipeKeyValueList(buildOrders());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        //when
        ReadOnlyKeyValueStore<String, Long> ordersCountPerStoreStore = topologyTestDriver.getKeyValueStore(ORDERS_COUNT_PER_STORE);
        //then
        var ordersCountStore1 = ordersCountPerStoreStore.get("store1");
        assertEquals(1, ordersCountStore1);

    }

    @Test
    void ordersCountPerMeatTypeTest() {
        //given
        try {
            inputTopic.pipeKeyValueList(buildOrders());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        //when
        ReadOnlyKeyValueStore<String, Long> ordersCountPerMeatTypeStore = topologyTestDriver.getKeyValueStore(
                ORDERS_COUNT_PER_MEAT_TYPE);
        //then
        var ordersCountStore1 = ordersCountPerMeatTypeStore.get("CHICKEN");
        assertEquals(3, ordersCountStore1);

    }

    @Test
    void ordersRevenueAmountPerStoreTest() {
        //given
        try {
            inputTopic.pipeKeyValueList(buildOrders());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        //when
        ReadOnlyKeyValueStore<String, TotalRevenuePerStore> ordersRevenuePerStoreStore = topologyTestDriver.getKeyValueStore(REVENUE_TOTAL_PER_STORE);

        //then
        var ordersRevenueStore1 = ordersRevenuePerStoreStore.get("store1");
        assertEquals(BigDecimal.valueOf(50.0), ordersRevenueStore1.getRunningRevenue());
    }

    @Test
    void ordersRevenueAmountPerMeatTypeTest() {
        //given
        try {
            inputTopic.pipeKeyValueList(buildOrders());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        //when
        ReadOnlyKeyValueStore<String, TotalRevenuePerMeatType> ordersRevenuePerMeatTypeStore = topologyTestDriver.getKeyValueStore(REVENUE_TOTAL_PER_MEAT_TYPE);

        //then
        var ordersRevenueStore1 = ordersRevenuePerMeatTypeStore.get("CHICKEN");
        assertEquals(BigDecimal.valueOf(150.0), ordersRevenueStore1.getRunningRevenue());
    }

    @Test
    void ordersRevenueAmountPerStorePerMeatTypeTest() {
        //given
        try {
            inputTopic.pipeKeyValueList(buildOrders());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        //when
        ReadOnlyKeyValueStore<String, TotalRevenuePerStorePerMeatType> ordersRevenuePerStorePerMeatTypeStore = topologyTestDriver.getKeyValueStore(REVENUE_TOTAL_PER_MEAT_TYPE_PER_STORE);

        //then
        var ordersRevenueStore1 = ordersRevenuePerStorePerMeatTypeStore.get("store1-CHICKEN");
        assertEquals(BigDecimal.valueOf(50.0), ordersRevenueStore1.getRunningRevenue());
    }

    public static List<KeyValue<String, String>> buildOrders() throws JsonProcessingException {

        var ordersList = List.of(
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, caramelised onions and chicken patty", BigDecimal.valueOf(50.0), "store1", MeatType.CHICKEN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, caramelised onions and chicken patty", BigDecimal.valueOf(50.0), "store2", MeatType.CHICKEN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce and soy patty", BigDecimal.valueOf(60.0), "store2", MeatType.VEGETARIAN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, cheese and beef patty", BigDecimal.valueOf(70.0), "store3", MeatType.BEEF),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce and soy patty", BigDecimal.valueOf(60.0), "store3", MeatType.VEGETARIAN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, caramelised onions and chicken patty", BigDecimal.valueOf(50.0), "store3", MeatType.CHICKEN));

        return List.of(
                KeyValue.pair(ordersList.get(0).getBarcodeNumber(), objectMapper.writeValueAsString(ordersList.get(0))),
                KeyValue.pair(ordersList.get(1).getBarcodeNumber(), objectMapper.writeValueAsString(ordersList.get(1))),
                KeyValue.pair(ordersList.get(2).getBarcodeNumber(), objectMapper.writeValueAsString(ordersList.get(2))),
                KeyValue.pair(ordersList.get(3).getBarcodeNumber(), objectMapper.writeValueAsString(ordersList.get(3))),
                KeyValue.pair(ordersList.get(4).getBarcodeNumber(), objectMapper.writeValueAsString(ordersList.get(4))),
                KeyValue.pair(ordersList.get(5).getBarcodeNumber(), objectMapper.writeValueAsString(ordersList.get(5))));
    }

}
