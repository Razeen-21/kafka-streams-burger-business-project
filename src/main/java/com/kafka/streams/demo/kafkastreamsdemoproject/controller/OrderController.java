package com.kafka.streams.demo.kafkastreamsdemoproject.controller;

import com.kafka.streams.demo.kafkastreamsdemoproject.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
@RequestMapping(value = "/v1/orders")
public class OrderController {

    private final OrderService orderService;
    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping(value = "/count/by-store")
    public ResponseEntity<?> retrieveOrdersCountByStore(
            @RequestParam(name="store_code",required = false) String storeCode,
            @RequestParam(name = "query_other_hosts",required = false) String queryOtherHosts){

        if(!StringUtils.hasText(queryOtherHosts)){
            queryOtherHosts="true";
        }

        if(Objects.isNull(storeCode)){

            var countOrdersByStore = orderService.getAllOrdersCountByStore(queryOtherHosts);

            //empty check
            if(countOrdersByStore.isEmpty()){
                return (ResponseEntity<?>) ResponseEntity.noContent();
            }

            return ResponseEntity.ok(countOrdersByStore);
        }

        return ResponseEntity.ok(orderService.getOrdersCountForOneStore(storeCode));
    }

    @GetMapping(value = "/count/by-meatType")
    public ResponseEntity<?> retrieveOrdersCountByMeatType(
            @RequestParam(name = "meat_type",required = false) String meatType,
            @RequestParam(name = "query_other_hosts",required = false) String queryOtherHosts) {

        if(!StringUtils.hasText(queryOtherHosts)){
            queryOtherHosts="true";
        }

        if (Objects.isNull(meatType)){
          return ResponseEntity.ok(orderService.getAllOrdersCountByMeatType(queryOtherHosts));
        }

        return ResponseEntity.ok(orderService.getOrdersCountForOneMeatType(meatType));
    }

    @GetMapping(value = "/revenue/by-store")
    public ResponseEntity<?> retrieveRevenueByStore(
            @RequestParam(name="store_code",required = false) String storeCode,
            @RequestParam(name = "query_other_hosts",required = false) String queryOtherHosts){

        if(!StringUtils.hasText(queryOtherHosts)){
            queryOtherHosts="true";
        }

        if(Objects.isNull(storeCode)){
            return ResponseEntity.ok(orderService.getRevenueForAllStores(queryOtherHosts));
        }

        return ResponseEntity.ok(orderService.getRevenueForOneStore(storeCode));
    }

    @GetMapping(value = "/revenue/by-meatType")
    public ResponseEntity<?> retrieveRevenueByMeatType(
            @RequestParam(name = "meat_type",required = false) String meatType,
            @RequestParam(name = "query_other_hosts",required = false) String queryOtherHosts) {

        if(!StringUtils.hasText(queryOtherHosts)){
            queryOtherHosts="true";
        }

        if (Objects.isNull(meatType)){
            return ResponseEntity.ok(orderService.getRevenueForAllMeatTypes(queryOtherHosts));
        }

        return ResponseEntity.ok(orderService.getRevenueForOneMeatType(meatType));
    }

    @GetMapping(value = "/revenue/by-store/by-meatType")
    public ResponseEntity<?> retrieveRevenueByStoreByMeatType(
            @RequestParam(name="store_code",required = false) String storeCode,
            @RequestParam(name = "meat_type",required = false) String meatType,
            @RequestParam(name = "query_other_hosts",required = false) String queryOtherHosts) {

        if(!StringUtils.hasText(queryOtherHosts)){
            queryOtherHosts="true";
        }

        if(Objects.isNull(meatType) && Objects.nonNull(storeCode)){
            return ResponseEntity.ok(orderService.getRevenueForAllMeatTypesByStore(storeCode,queryOtherHosts));
        }
        else if(Objects.isNull(storeCode) && Objects.nonNull(meatType)){
            return ResponseEntity.ok(orderService.getRevenueForAllStoresByMeatType(meatType,queryOtherHosts));
        }

        return ResponseEntity.ok(orderService.getRevenueForAllStoresForAllMeatTypes(queryOtherHosts));

    }
}
