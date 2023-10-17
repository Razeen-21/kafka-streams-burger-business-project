package com.kafka.streams.demo.kafkastreamsdemoproject.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.MeatType;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.Order;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static com.kafka.streams.demo.kafkastreamsdemoproject.producer.ProducerUtil.publishMessageSync;
import static java.lang.Thread.sleep;

@Slf4j
public class OrdersProducer {

    public static String ORDERS = "orders";

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        produceOrders(objectMapper, buildOrders());

        log.info("Orders produced at time... {}", LocalDateTime.now());

//        System.out.println(LocalDateTime.now());
//
//        var timeStamp = LocalDateTime.now();
//        //var timeStamp = LocalDateTime.now(ZoneId.of("UTC"));
//        System.out.println("timeStamp : " + timeStamp);
//
//        var instant = timeStamp.toEpochSecond(ZoneOffset.ofHours(-6));
//        System.out.println("instant : " + instant);
    }

    private static void produceOrders(ObjectMapper objectMapper, List<Order> orders) {
        orders.forEach(order -> {
                    try {
                        var ordersString = objectMapper.writeValueAsString(order);
                        var recordMetaData = publishMessageSync(ORDERS, order.getBarcodeNumber(), ordersString);
                        log.info("Published a new order message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    private static List<Order> buildOrders() {

        return List.of(
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, caramelised onions and chicken patty", BigDecimal.valueOf(50.0), "store1", MeatType.CHICKEN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, caramelised onions and chicken patty", BigDecimal.valueOf(50.0), "store2", MeatType.CHICKEN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce and soy patty", BigDecimal.valueOf(60.0), "store2", MeatType.VEGETARIAN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, cheese and beef patty", BigDecimal.valueOf(70.0), "store3", MeatType.BEEF),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce and soy patty", BigDecimal.valueOf(60.0), "store3", MeatType.VEGETARIAN),
                new Order(generateBarcode(), "Toasted bun, tomato, lettuce, caramelised onions and chicken patty", BigDecimal.valueOf(50.0), "store3", MeatType.CHICKEN));
    }

    private static void produceBulkOrders(ObjectMapper objectMapper) throws InterruptedException {

        int count = 0;
        while (count < 100) {
            var orders = buildOrders();
            produceOrders(objectMapper, orders);
            sleep(1000);
            count++;
        }
    }

    private static String generateBarcode() {
        var barCode = UUID.randomUUID();
        var removeUnderscores = barCode.toString().replace("_", "0");
        var removeDashes = removeUnderscores.replace("-", "0");
        return removeDashes.substring(0, 16);
    }

}
