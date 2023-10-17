package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@Getter
@Setter
@ToString
public class TotalRevenuePerMeatType {

    private String meatType;
    private Integer orderCount;
    private BigDecimal runningRevenue;

    public TotalRevenuePerMeatType(){
        this.meatType="";
        this.orderCount=0;
        this.runningRevenue=BigDecimal.ZERO;
    }

    public TotalRevenuePerMeatType (String meatType, Integer orderCount, BigDecimal runningRevenue){
        this.meatType=meatType;
        this.orderCount=orderCount;
        this.runningRevenue=runningRevenue;
    }

    public TotalRevenuePerMeatType calculateRunningRevenue(String key, Order order){
        var newOrdersCount=this.orderCount+1;
        var newRevenue = this.runningRevenue.add(order.getPrice());
        return new TotalRevenuePerMeatType(key,newOrdersCount,newRevenue);
    }

}
