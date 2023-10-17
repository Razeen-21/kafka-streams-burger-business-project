package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class KeyHostInfoDTO {

    private String host;
    private int port;
    private String key;

}
