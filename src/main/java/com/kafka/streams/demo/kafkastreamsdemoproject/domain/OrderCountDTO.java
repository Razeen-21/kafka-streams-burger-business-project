package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class OrderCountDTO {
    private String key;
    private Long orderCount;
}
