package com.kafka.streams.demo.kafkastreamsdemoproject.client;

import com.kafka.streams.demo.kafkastreamsdemoproject.domain.HostInfoDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.KeyHostInfoDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderCountDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenueMeatTypeDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenueStoreDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.RevenueAllMeatTypesPerStoreDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.RevenueAllStoresPerMeatTypeDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.RevenuePerStorePerMeatTypeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;

@Component
@Slf4j
public class OrderServiceClient {

    private static final String ORDERS_COUNT_BY_STORE_PATH = "/v1/orders/count/by-store";
    private static final String ORDERS_COUNT_BY_MEAT_TYPE_PATH = "/v1/orders/count/by-meatType";

    private static final String ORDERS_REVENUE_BY_STORE_PATH = "/v1/orders/revenue/by-store";
    private static final String ORDERS_REVENUE_BY_MEAT_TYPE_PATH = "/v1/orders/revenue/by-meatType";
    private static final String ORDERS_REVENUE_BY_STORE_BY_MEAT_TYPE_PATH = "/v1/orders/revenue/by-store/by-meatType";
    private final WebClient webClient;

    public OrderServiceClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public ArrayList<OrderCountDTO> getAllOrdersCountByStore(HostInfoDTO hostInfoDTO, String storeCode) {
        var basePath = "http://"+hostInfoDTO.getHost()+":"+hostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_COUNT_BY_STORE_PATH)
                .queryParam("query_other_hosts", false)
                .buildAndExpand(storeCode)
                .toString();

        return (ArrayList<OrderCountDTO>) webClient.get()
                .uri(url)
                .retrieve()
                .bodyToFlux(OrderCountDTO.class)
                .collectList()
                .block();
    }

    public OrderCountDTO getAllOrdersCountForOneStore(KeyHostInfoDTO keyHostInfoDTO, String storeCode){
        var basePath = "http://"+keyHostInfoDTO.getHost()+":"+keyHostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_COUNT_BY_STORE_PATH)
                .queryParam("query_other_hosts", false)
                .queryParam("store_code", storeCode)
                .buildAndExpand(storeCode)
                .toString();

        return  webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(OrderCountDTO.class)
                .block();
    }

    public ArrayList<OrderCountDTO> getAllOrdersCountByMeatType(HostInfoDTO hostInfoDTO, String meatType) {
        var basePath = "http://"+hostInfoDTO.getHost()+":"+hostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_COUNT_BY_MEAT_TYPE_PATH)
                .queryParam("query_other_hosts", false)
                .buildAndExpand(meatType)
                .toString();

        return (ArrayList<OrderCountDTO>) webClient.get()
                .uri(url)
                .retrieve()
                .bodyToFlux(OrderCountDTO.class)
                .collectList()
                .block();
    }

    public OrderCountDTO getAllOrdersCountForOneMeatType(KeyHostInfoDTO keyHostInfoDTO, String meatType){
        var basePath = "http://"+keyHostInfoDTO.getHost()+":"+keyHostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_COUNT_BY_MEAT_TYPE_PATH)
                .queryParam("query_other_hosts", false)
                .queryParam("meat_type", meatType)
                .buildAndExpand(meatType)
                .toString();

        return  webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(OrderCountDTO.class)
                .block();
    }

    public ArrayList<OrderRevenueStoreDTO> getAllRevenueByStore(HostInfoDTO hostInfoDTO, String store) {
        var basePath = "http://"+hostInfoDTO.getHost()+":"+hostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_REVENUE_BY_STORE_PATH)
                .queryParam("query_other_hosts", false)
                .buildAndExpand(store)
                .toString();

        return (ArrayList<OrderRevenueStoreDTO>) webClient.get()
                .uri(url)
                .retrieve()
                .bodyToFlux(OrderRevenueStoreDTO.class)
                .collectList()
                .block();
    }

    public ArrayList<OrderRevenueMeatTypeDTO> getAllRevenueByMeatType(HostInfoDTO hostInfoDTO, String meatType) {
        var basePath = "http://"+hostInfoDTO.getHost()+":"+hostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_REVENUE_BY_MEAT_TYPE_PATH)
                .queryParam("query_other_hosts", false)
                .buildAndExpand(meatType)
                .toString();

        return (ArrayList<OrderRevenueMeatTypeDTO>) webClient.get()
                .uri(url)
                .retrieve()
                .bodyToFlux(OrderRevenueMeatTypeDTO.class)
                .collectList()
                .block();
    }

    public OrderRevenueStoreDTO getAllRevenueForOneStore(KeyHostInfoDTO hostInfoDTO, String storeCode) {
        var basePath = "http://"+hostInfoDTO.getHost()+":"+hostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_REVENUE_BY_STORE_PATH)
                .queryParam("query_other_hosts", false)
                .queryParam("store_code", storeCode)
                .buildAndExpand(storeCode)
                .toString();

        return  webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(OrderRevenueStoreDTO.class)
                .block();
    }

    public OrderRevenueMeatTypeDTO getAllRevenueForOneMeatType(KeyHostInfoDTO hostInfoDTO, String meatType) {
        var basePath = "http://"+hostInfoDTO.getHost()+":"+hostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_REVENUE_BY_MEAT_TYPE_PATH)
                .queryParam("query_other_hosts", false)
                .queryParam("meat_type", meatType)
                .buildAndExpand(meatType)
                .toString();

        return  webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(OrderRevenueMeatTypeDTO.class)
                .block();
    }

    public ArrayList<RevenuePerStorePerMeatTypeDTO> getRevenueForAllStoresForAllMeatTypes(HostInfoDTO hostInfoDTO, String storeCode, String meatType){

        var basePath="http://"+hostInfoDTO.getHost()+":"+hostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_REVENUE_BY_STORE_BY_MEAT_TYPE_PATH)
                .queryParam("query_other_hosts",false)
                .queryParam("store_code", storeCode)
                .queryParam("meat_type",meatType)
                .buildAndExpand(meatType,storeCode)
                .toString();

        log.info("REVENUE ALL MEAT TYPES ALL STORES URL {}", url);

        return (ArrayList<RevenuePerStorePerMeatTypeDTO>) webClient.get()
                .uri(url)
                .retrieve()
                .bodyToFlux(RevenuePerStorePerMeatTypeDTO.class)
                .collectList()
                .block();

    }

    public RevenueAllStoresPerMeatTypeDTO getRevenueAllStoresPerMeatType(KeyHostInfoDTO keyHostInfoDTO, String meatType){
        var basePath="http://"+keyHostInfoDTO.getHost()+":"+keyHostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_REVENUE_BY_STORE_BY_MEAT_TYPE_PATH)
                .queryParam("query_other_hosts",false)
                .queryParam("meat_type", meatType)
                .buildAndExpand(meatType)
                .toString();

        log.info("REVENUE ALL STORES  TYPES MEAT-TYPE URL {}", url);

        var response = webClient.get().uri(url).retrieve().bodyToMono(RevenueAllStoresPerMeatTypeDTO.class).block();
        log.info("RESPONSE {}", response);

        return response;
    }

    public RevenueAllMeatTypesPerStoreDTO getRevenueAllMeatTypesPerStore(KeyHostInfoDTO keyHostInfoDTO, String storeCode){
        var basePath="http://"+keyHostInfoDTO.getHost()+":"+keyHostInfoDTO.getPort();

        var url = UriComponentsBuilder.fromHttpUrl(basePath)
                .path(ORDERS_REVENUE_BY_STORE_BY_MEAT_TYPE_PATH)
                .queryParam("query_other_hosts",false)
                .queryParam("store_code", storeCode)
                .queryParam("meat_type","")
                .buildAndExpand(storeCode)
                .toString();

        log.info("REVENUE ALL MEAT TYPER PER STORE URL {}", url);

        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(RevenueAllMeatTypesPerStoreDTO.class)
                .block();
    }




}
