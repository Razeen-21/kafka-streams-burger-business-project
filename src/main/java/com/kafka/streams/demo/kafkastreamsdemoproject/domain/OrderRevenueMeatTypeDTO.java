package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class OrderRevenueMeatTypeDTO {

    private String meatType;
    private OrderRevenue revenue;
}
