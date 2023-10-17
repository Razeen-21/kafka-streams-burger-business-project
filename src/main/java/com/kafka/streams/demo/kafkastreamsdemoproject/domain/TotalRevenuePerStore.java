package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@Getter
@Setter
@ToString
public class TotalRevenuePerStore {

    private String storeCode;

    private Integer ordersCount;
    private BigDecimal runningRevenue;

    public TotalRevenuePerStore(){
        this.storeCode="";
        this.ordersCount=0;
        this.runningRevenue=BigDecimal.ZERO;
    }

    public TotalRevenuePerStore(String key, int newOrdersCount, BigDecimal newRevenue) {
        this.storeCode=key;
        this.ordersCount=newOrdersCount;
        this.runningRevenue=newRevenue;
    }

    public TotalRevenuePerStore incrementTotalRevenuePerStore(String key, Order order) {
        var newOrdersCount = this.ordersCount+1;
        var newRevenue = this.runningRevenue.add(order.getPrice());
        return new TotalRevenuePerStore(key,newOrdersCount,newRevenue);
    }



}
