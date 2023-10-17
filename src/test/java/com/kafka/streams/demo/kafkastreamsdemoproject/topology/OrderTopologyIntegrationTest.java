package com.kafka.streams.demo.kafkastreamsdemoproject.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.streams.demo.kafkastreamsdemoproject.service.OrderService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.KafkaStreamsTopology.ORDERS;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderTopologyTest.buildOrders;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(topics = {ORDERS})
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class OrderTopologyIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OrderService orderService;

    @BeforeEach
    void setUp(){
        streamsBuilderFactoryBean.start();
    }

    @AfterEach
    void tearDown(){
        Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).close();
        streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
    }

    @Test
    void ordersCountPerStore() {
        //given
        publishOrders();
        //then
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getAllOrdersCountByStore("false").get(0).getOrderCount(), greaterThan(1L));
        assertEquals("store1",orderService.getAllOrdersCountByStore("false").get(2).getKey());
        assertEquals("store3",orderService.getAllOrdersCountByStore("false").get(1).getKey());
        assertEquals("store2",orderService.getAllOrdersCountByStore("false").get(0).getKey());
        assertEquals(3,orderService.getAllOrdersCountByStore("false").size());
    }

    @Test
    void ordersCountPerStoreSpecific(){
        publishOrders();

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCountForOneStore("store1").getOrderCount(), equalTo(1L));

        assertEquals("store1", orderService.getOrdersCountForOneStore("store1").getKey());
    }

    @Test
    void ordersCountPerMeatType() {
        //given
        publishOrders();
        //then
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getAllOrdersCountByMeatType("false").get(0).getOrderCount(), equalTo(1L));
        assertEquals("CHICKEN",orderService.getAllOrdersCountByMeatType("false").get(2).getKey());
        assertEquals("VEGETARIAN",orderService.getAllOrdersCountByMeatType("false").get(1).getKey());
        assertEquals("BEEF",orderService.getAllOrdersCountByMeatType("false").get(0).getKey());
        assertEquals(3,orderService.getAllOrdersCountByMeatType("false").size());
    }

    @Test
    void ordersCountPerMeatTypeSpecific(){
        publishOrders();

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCountForOneMeatType("CHICKEN").getOrderCount(), equalTo(3L));

        assertEquals("CHICKEN", orderService.getOrdersCountForOneMeatType("CHICKEN").getKey());
    }

    @Test
    void ordersRevenueByStore(){
        publishOrders();

        Awaitility.await().atMost(10,TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getRevenueForAllStores("false").get(0).getRevenue().getRunningRevenue(), greaterThan(BigDecimal.ZERO));
        assertEquals(3,orderService.getRevenueForAllStores("false").size());
        assertEquals(BigDecimal.valueOf(110.0),orderService.getRevenueForAllStores("false").get(0).getRevenue().getRunningRevenue());
    }

    @Test
    void ordersRevenueByStoreSpecific(){
        publishOrders();

        Awaitility.await().atMost(10,TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getRevenueForOneStore("store1").getRevenue().getRunningRevenue(), equalTo(BigDecimal.valueOf(50.0)));
        assertEquals(BigDecimal.valueOf(50.0),orderService.getRevenueForOneStore("store1").getRevenue().getRunningRevenue());
    }

    @Test
    void ordersRevenueByMeatType(){
        publishOrders();

        Awaitility.await().atMost(10,TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getRevenueForAllMeatTypes("false").get(0).getRevenue().getRunningRevenue(), greaterThan(BigDecimal.valueOf(0.0)));
        assertEquals(3,orderService.getRevenueForAllMeatTypes("false").size());
        assertEquals(BigDecimal.valueOf(150.0),orderService.getRevenueForAllMeatTypes("false").get(2).getRevenue().getRunningRevenue());
    }

    @Test
    void ordersRevenueByMeatTypeSpecific(){
        publishOrders();

        Awaitility.await().atMost(10,TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getRevenueForOneMeatType("CHICKEN").getRevenue().getRunningRevenue(), equalTo(BigDecimal.valueOf(150.0)));
        assertEquals(BigDecimal.valueOf(150.0),orderService.getRevenueForOneMeatType("CHICKEN").getRevenue().getRunningRevenue());
    }


    @Test
    void ordersRevenueByMeatTypeByStore(){
        publishOrders();

        Awaitility.await().atMost(10,TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getRevenueForAllStoresForAllMeatTypes("false").get(0).getRevenue().getRunningRevenue(), greaterThan(BigDecimal.valueOf(0.0)));
        assertEquals(6,orderService.getRevenueForAllStoresForAllMeatTypes("false").size());
    }

    @Test
    void ordersRevenueByMeatTypeAllStoreSpecificMeatType(){
        publishOrders();

        Awaitility.await().atMost(10,TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getRevenueForAllStoresByMeatType("CHICKEN","false").get(0).getRevenue().getRunningRevenue(), greaterThan(BigDecimal.valueOf(0.0)));
        assertEquals(BigDecimal.valueOf(50.0),orderService.getRevenueForAllStoresByMeatType("CHICKEN","false").get(0).getRevenue().getRunningRevenue());
    }

    @Test
    void ordersRevenueAllMeatTypeByStoreSpecificStore(){
        publishOrders();

        Awaitility.await().atMost(10,TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getRevenueForAllMeatTypesByStore("store1","false").get(0).getRevenue().getRunningRevenue(), greaterThan(BigDecimal.valueOf(0.0)));
        assertEquals(BigDecimal.valueOf(50.0),orderService.getRevenueForAllMeatTypesByStore("store1","false").get(0).getRevenue().getRunningRevenue());
    }


    private void publishOrders() {
        try {
            buildOrders().forEach(order -> {
                kafkaTemplate.send(ORDERS, order.key, order.value);
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
