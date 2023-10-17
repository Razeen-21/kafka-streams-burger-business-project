package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class RevenueAllMeatTypesPerStoreDTO {

    private String storeCode;
    private HashMap<String, OrderRevenue> revenueDetailsPerStore;
}
