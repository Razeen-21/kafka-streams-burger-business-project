package com.kafka.streams.demo.kafkastreamsdemoproject.controller;


import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderCountDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenue;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenueMeatTypeDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenueStoreDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.RevenuePerStorePerMeatTypeDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.service.OrderService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderStreamsTopology.ORDERS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@EmbeddedKafka(topics = {ORDERS})
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class OrderControllerTest {

    @Mock
    OrderService orderService;
    MockMvc mockMvc;
    @InjectMocks
    OrderController orderController;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(orderController).build();
    }

    @AfterEach
    void tearDown(){
        Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).close();
        streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
    }

    @Test
    void getOrdersCountPerStore() throws Exception {

        when(orderService.getAllOrdersCountByStore("false")).thenReturn(orderCountPerStoreDTOResponse());

        var url = "/v1/orders/count/by-store";
        mockMvc.perform(MockMvcRequestBuilders.get(url))
                .andExpect(status().isOk())
                .andReturn();

    }

    @Test
    void getOrdersCountPerMeatType() throws Exception {

        when(orderService.getAllOrdersCountByMeatType("false")).thenReturn(orderCountPerMeatTypeDTOResponse());

        var url = "/v1/orders/count/by-meatType";
        mockMvc.perform(MockMvcRequestBuilders.get(url))
                .andExpect(status().isOk())
                .andReturn();

    }

    @Test
    void getOrdersCountPerMeatTypeSpecificMeatType() throws Exception {

        when(orderService.getOrdersCountForOneMeatType(anyString())).thenReturn(new OrderCountDTO("CHICKEN", 1L));

        var url = "/v1/orders/count/by-meatType";
        mockMvc.perform(MockMvcRequestBuilders.get(url).requestAttr("meat_type", "CHICKEN"))
                .andExpect(status().isOk())
                .andReturn();

    }

    @Test
    void getOrdersCountPerStoreSpecificStore() throws Exception {

        when(orderService.getOrdersCountForOneStore(anyString())).thenReturn(new OrderCountDTO("store1", 1L));

        var url = "/v1/orders/count/by-store";
        mockMvc.perform(MockMvcRequestBuilders.get(url).requestAttr("store_code", "store1"))
                .andExpect(status().isOk())
                .andReturn();

    }

    @Test
    void getRevenueAllStores() throws Exception {

        when(orderService.getRevenueForAllStores("false")).thenReturn(totalRevenuePerStoreDTOResponse());

        var url = "/v1/orders/revenue/by-store";
        mockMvc.perform(MockMvcRequestBuilders.get(url))
                .andExpect(status().isOk())
                .andReturn();

    }

    @Test
    void getRevenuePerStoreSpecific() throws Exception {

        when(orderService.getRevenueForOneStore(anyString())).thenReturn(new OrderRevenueStoreDTO("store1", new OrderRevenue(1, BigDecimal.valueOf(50.0))));

        var url = "/v1/orders/revenue/by-store";
        mockMvc.perform(MockMvcRequestBuilders.get(url).requestAttr("store_code", "store1"))
                .andExpect(status().isOk())
                .andReturn();
    }

    @Test
    void getRevenueAllMeatTypes() throws Exception {

        when(orderService.getRevenueForAllMeatTypes("false")).thenReturn(totalRevenuePerMeatTypeDTOResponse());

        var url = "/v1/orders/revenue/by-meatType";
        mockMvc.perform(MockMvcRequestBuilders.get(url))
                .andExpect(status().isOk())
                .andReturn();
    }

    @Test
    void getRevenuePerMeatTypesSpecific() throws Exception {

        when(orderService.getRevenueForOneMeatType(anyString())).thenReturn(new OrderRevenueMeatTypeDTO("CHICKEN", new OrderRevenue(1, BigDecimal.valueOf(50.0))));

        var url = "/v1/orders/revenue/by-meatType";
        mockMvc.perform(MockMvcRequestBuilders.get(url).requestAttr("meat_type", "CHICKEN"))
                .andExpect(status().isOk())
                .andReturn();
    }

    @Test
    void getRevenueAllMeatTypesAllStores() throws Exception {
        when(orderService.getRevenueForAllStoresForAllMeatTypes(anyString())).thenReturn(totalRevenuePerStorePerMeatTypeDTOResponse());

        var url = "/v1/orders/revenue/by-store/by-meatType";
        mockMvc.perform(MockMvcRequestBuilders.get(url))
                .andExpect(status().isOk())
                .andReturn();
    }

    @Test
    void getRevenueAllMeatTypeByStore() throws Exception {

        when(orderService.getRevenueForAllMeatTypesByStore(anyString(),anyString())).thenReturn(List.of(new RevenuePerStorePerMeatTypeDTO("store1", "CHICKEN" ,
                new OrderRevenue(1,BigDecimal.ONE))));

        var url = "/v1/orders/revenue/by-store/by-meatType";
        mockMvc.perform(MockMvcRequestBuilders.get(url).requestAttr("store_code", "store1"))
                .andExpect(status().isOk())
                .andReturn();
    }

    @Test
    void getRevenueAllStoresByMeatType() throws Exception {

        var revenueByStoresHashMap = new HashMap<String,OrderRevenue>();
        revenueByStoresHashMap.put("store1", new OrderRevenue(1,BigDecimal.ONE));

        when(orderService.getRevenueForAllStoresByMeatType(anyString(),anyString())).thenReturn(List.of(new RevenuePerStorePerMeatTypeDTO("store1", "CHICKEN" ,
                new OrderRevenue(1,BigDecimal.ONE))));

        var url = "/v1/orders/revenue/by-store/by-meatType";
        mockMvc.perform(MockMvcRequestBuilders.get(url).requestAttr("meat_type", "CHICKEN"))
                .andExpect(status().isOk())
                .andReturn();
    }

    List<OrderRevenueStoreDTO> totalRevenuePerStoreDTOResponse() {
        var revenuePerStoreDTOList = new ArrayList<OrderRevenueStoreDTO>();
        revenuePerStoreDTOList.add(new OrderRevenueStoreDTO("store1", new OrderRevenue(1, BigDecimal.valueOf(50.0))));
        return revenuePerStoreDTOList;

    }

    ArrayList<OrderRevenueMeatTypeDTO> totalRevenuePerMeatTypeDTOResponse() {
        var orderRevenueMeatTypeDTOS = new ArrayList<OrderRevenueMeatTypeDTO>();
        orderRevenueMeatTypeDTOS.add(new OrderRevenueMeatTypeDTO("CHICKEN", new OrderRevenue(1, BigDecimal.valueOf(50.0))));
        return orderRevenueMeatTypeDTOS;
    }

    ArrayList<RevenuePerStorePerMeatTypeDTO> totalRevenuePerStorePerMeatTypeDTOResponse() {
        var orderRevenuePerMeatTypePerStoreDTOS = new ArrayList<RevenuePerStorePerMeatTypeDTO>();
        orderRevenuePerMeatTypePerStoreDTOS.add(new RevenuePerStorePerMeatTypeDTO("store1","CHICKEN", new OrderRevenue(1, BigDecimal.valueOf(50.0))));
        return orderRevenuePerMeatTypePerStoreDTOS;
    }

    ArrayList<OrderCountDTO> orderCountPerStoreDTOResponse() {
        var ordersCountPerStoreDTOList = new ArrayList<OrderCountDTO>();
        ordersCountPerStoreDTOList.add(new OrderCountDTO("store1", 1L));
        return ordersCountPerStoreDTOList;
    }

    ArrayList<OrderCountDTO> orderCountPerMeatTypeDTOResponse() {
        var ordersCountPerMeatTypeDTOList = new ArrayList<OrderCountDTO>();
        ordersCountPerMeatTypeDTOList.add(new OrderCountDTO("meatType", 1L));
        return ordersCountPerMeatTypeDTOList;
    }


}
