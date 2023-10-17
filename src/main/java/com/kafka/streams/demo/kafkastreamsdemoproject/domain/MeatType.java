package com.kafka.streams.demo.kafkastreamsdemoproject.domain;

public enum MeatType {

    CHICKEN("CHICKEN"),
    BEEF("BEEF"),
    VEGETARIAN("VEGETARIAN");

    MeatType(String value) {
    }

    public String getMeatType(MeatType meatType){
        return meatType.name();
    }
}
