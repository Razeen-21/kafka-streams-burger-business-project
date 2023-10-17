package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class RevenuePerStorePerMeatTypeDTO {
    private String storeCode;
    private String meatType;
    private OrderRevenue revenue;
}
