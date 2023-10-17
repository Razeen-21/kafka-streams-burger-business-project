package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.streams.kstream.Predicate;

import java.math.BigDecimal;
import java.util.HashMap;

@Getter
@Setter
@ToString
public class TotalRevenuePerStorePerMeatType {

    private Integer ordersCount;
    private BigDecimal runningRevenue;

    public TotalRevenuePerStorePerMeatType(){
        this.ordersCount=0;
        this.runningRevenue=BigDecimal.ZERO;
    }

    public TotalRevenuePerStorePerMeatType(int newOrdersCount, BigDecimal newRevenue) {
        this.ordersCount=newOrdersCount;
        this.runningRevenue=newRevenue;
    }

    public TotalRevenuePerStorePerMeatType incrementTotalRevenuePerStore(Order order) {
        var newOrdersCount = this.ordersCount+1;
        var newRevenue = this.runningRevenue.add(order.getPrice());
        return new TotalRevenuePerStorePerMeatType(newOrdersCount,newRevenue);
    }

}
