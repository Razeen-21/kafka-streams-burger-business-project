package com.kafka.streams.demo.kafkastreamsdemoproject.service;

import com.kafka.streams.demo.kafkastreamsdemoproject.client.OrderServiceClient;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.HostInfoDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderCountDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenue;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenueMeatTypeDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.OrderRevenueStoreDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.RevenuePerStorePerMeatTypeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderStreamsTopology.ORDERS_COUNT_PER_MEAT_TYPE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderStreamsTopology.ORDERS_COUNT_PER_STORE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderStreamsTopology.REVENUE_TOTAL_PER_MEAT_TYPE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderStreamsTopology.REVENUE_TOTAL_PER_MEAT_TYPE_PER_STORE;
import static com.kafka.streams.demo.kafkastreamsdemoproject.topology.OrderStreamsTopology.REVENUE_TOTAL_PER_STORE;

@Service
@Slf4j
public class OrderService {

    private final OrderStoreService orderStoreService;

    private final OrderServiceClient orderServiceClient;
    private final MetaDataService metaDataService;

    @Value("${server.port}")
    private Integer currentInstancePort;

    public OrderService(OrderStoreService orderStoreService, OrderServiceClient orderServiceClient, MetaDataService metaDataService) {
        this.orderStoreService = orderStoreService;
        this.orderServiceClient = orderServiceClient;
        this.metaDataService = metaDataService;
    }

    public ArrayList<OrderCountDTO> getAllOrdersCountByStore(String queryOtherHosts) {
        var ordersCountStore = orderStoreService.ordersStore(ORDERS_COUNT_PER_STORE);
        var ordersCount = ordersCountStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(ordersCount, 0);

        var currentInstanceAggregatedData = (ArrayList<OrderCountDTO>) StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrderCountDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

        //fetch metadata from other instances
        log.info("QUERY OTHER HOSTS: {}", queryOtherHosts);

        if(Boolean.parseBoolean(queryOtherHosts)) {
            var orderCountPerStoreDTOList = retrieveOrderCountByStoreFromOtherInstances();

            return (ArrayList<OrderCountDTO>) Stream.of(currentInstanceAggregatedData, orderCountPerStoreDTOList)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        return currentInstanceAggregatedData;
    }

    private ArrayList<OrderCountDTO> retrieveOrderCountByStoreFromOtherInstances() {
        var otherKafkaStreamsHostInstances = otherHosts();
        log.info("otherHosts: {} ", otherKafkaStreamsHostInstances);

        if (!otherKafkaStreamsHostInstances.isEmpty()) {
            return (ArrayList<OrderCountDTO>) otherHosts().stream().map(hostInfoDTO ->
                            orderServiceClient.getAllOrdersCountByStore(hostInfoDTO, ""))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        return null;
    }

    private ArrayList<OrderCountDTO> retrieveOrderCountByMeatTypeFromOtherInstances() {
        var otherKafkaStreamsHostInstances = otherHosts();
        log.info("otherHosts: {} ", otherKafkaStreamsHostInstances);

        if (!otherKafkaStreamsHostInstances.isEmpty()) {
            return (ArrayList<OrderCountDTO>) otherHosts().stream().map(hostInfoDTO ->
                            orderServiceClient.getAllOrdersCountByMeatType(hostInfoDTO, ""))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        return null;
    }

    private ArrayList<OrderRevenueStoreDTO> retrieveRevenueByStoreFromOtherInstances() {
        var otherKafkaStreamsHostInstances = otherHosts();
        log.info("otherHosts: {}", otherKafkaStreamsHostInstances);

        if(!otherKafkaStreamsHostInstances.isEmpty()){
            return (ArrayList<OrderRevenueStoreDTO>) otherHosts()
                    .stream()
                    .map(hostInfoDTO -> orderServiceClient.getAllRevenueByStore(hostInfoDTO,""))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

        }
        return null;
    }

    private ArrayList<OrderRevenueMeatTypeDTO> retrieveRevenueByMeatTypeFromOtherInstances() {

        var otherKafkaStreamsHostInstances = otherHosts();
        log.info("otherHosts: {}", otherKafkaStreamsHostInstances);

        if(!otherKafkaStreamsHostInstances.isEmpty()){
            return (ArrayList<OrderRevenueMeatTypeDTO>) otherHosts()
                    .stream()
                    .map(hostInfoDTO -> orderServiceClient.getAllRevenueByMeatType(hostInfoDTO,""))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

        }
        return null;

    }

    private List<HostInfoDTO> otherHosts() {
        return metaDataService
                .getStreamsMetaData()
                .stream()
                .filter(hostInfoDTO -> hostInfoDTO.getPort() != currentInstancePort)
                .collect(Collectors.toList());
    }

    public OrderCountDTO getOrdersCountForOneStore(String storeCode) {

        var hostMetaDataByKey = metaDataService.getStreamsMetaDataByKey(ORDERS_COUNT_PER_STORE, storeCode);
        log.info("HOST META DATA CONTAINING WHAT WE NEED: {}", hostMetaDataByKey);

        if (Objects.nonNull(hostMetaDataByKey)) {
            if (hostMetaDataByKey.getPort() == currentInstancePort) {
                log.info("RETRIEVING DATA FROM CURRENT INSTANCE...");
                var ordersCountStore = orderStoreService.ordersStore(ORDERS_COUNT_PER_STORE);
                var ordersCountByStoreValue = ordersCountStore.get(storeCode);
                return new OrderCountDTO(storeCode, ordersCountByStoreValue);
            } else {
                log.info("RETRIEVING DATA FROM REMOTE INSTANCE...");
                //get data from other remote instance
                return orderServiceClient.getAllOrdersCountForOneStore(hostMetaDataByKey, storeCode);
            }
        }

        return null;

    }

    public ArrayList<OrderCountDTO> getAllOrdersCountByMeatType(String queryOtherHosts) {
        var ordersCountMeatTypeStore = orderStoreService.ordersStore(ORDERS_COUNT_PER_MEAT_TYPE);
        var ordersCount = ordersCountMeatTypeStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(ordersCount, 0);

        var ordersCountFromCurrentInstance =  (ArrayList<OrderCountDTO>) StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue -> new OrderCountDTO(stringLongKeyValue.key, stringLongKeyValue.value))
                .collect(Collectors.toList());

        if (Boolean.parseBoolean(queryOtherHosts)) {
            var ordersCountFromOtherInstances = retrieveOrderCountByMeatTypeFromOtherInstances();

            return (ArrayList<OrderCountDTO>) Stream.of(ordersCountFromCurrentInstance, ordersCountFromOtherInstances)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        return ordersCountFromCurrentInstance;

    }

    public OrderCountDTO getOrdersCountForOneMeatType(String meatType) {

        var otherHostsInfoByKey = metaDataService.getStreamsMetaDataByKey(ORDERS_COUNT_PER_MEAT_TYPE, meatType);

        if(Objects.nonNull(otherHostsInfoByKey)){
            if(otherHostsInfoByKey.getPort() == currentInstancePort){
                log.info("RETRIEVING DATA FROM CURRENT INSTANCE...");
                var ordersCountStore = orderStoreService.ordersStore(ORDERS_COUNT_PER_MEAT_TYPE);
                var ordersCountByMeatTypeValue = ordersCountStore.get(meatType);
                return new OrderCountDTO(meatType, ordersCountByMeatTypeValue);
            }
            else{
                log.info("RETRIEVING DATA FROM REMOTE INSTANCE...");
                return orderServiceClient.getAllOrdersCountForOneMeatType(otherHostsInfoByKey,meatType);
            }
        }
        return null;
    }

    public List<OrderRevenueStoreDTO> getRevenueForAllStores(String queryOtherHosts) {

        var totalRevenueByStoreStore = orderStoreService.ordersRevenuePerStoreStore(REVENUE_TOTAL_PER_STORE);
        var totalRevenueByStore = totalRevenueByStoreStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(totalRevenueByStore, 0);

        var revenueFromCurrentInstance = StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue -> new OrderRevenueStoreDTO(stringLongKeyValue.key,
                        new OrderRevenue(stringLongKeyValue.value.getOrdersCount(), stringLongKeyValue.value.getRunningRevenue())))
                .collect(Collectors.toList());

        if(Boolean.parseBoolean(queryOtherHosts)){

            var revenueFromOtherInstances = retrieveRevenueByStoreFromOtherInstances();

            return Stream.of(revenueFromCurrentInstance, revenueFromOtherInstances)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        return revenueFromCurrentInstance;

    }

    public OrderRevenueStoreDTO getRevenueForOneStore(String storeCode) {

        var otherHostsInfoByKey = metaDataService.getStreamsMetaDataByKey(REVENUE_TOTAL_PER_STORE,storeCode);

        if(Objects.nonNull(otherHostsInfoByKey)){
            if(otherHostsInfoByKey.getPort()==currentInstancePort){
                log.info("RETRIEVING DATA FROM CURRENT INSTANCE...");
                var totalRevenueByStoreStore = orderStoreService.ordersRevenuePerStoreStore(REVENUE_TOTAL_PER_STORE);
                var totalRevenueByStore = totalRevenueByStoreStore.get(storeCode);
                return new OrderRevenueStoreDTO(storeCode, new OrderRevenue(totalRevenueByStore.getOrdersCount(), totalRevenueByStore.getRunningRevenue()));
            }
            else{
                log.info("RETRIEVING DATA FROM REMOTE INSTANCE...");
                return orderServiceClient.getAllRevenueForOneStore(otherHostsInfoByKey,storeCode);
            }
        }
        return null;
    }

    public ArrayList<OrderRevenueMeatTypeDTO> getRevenueForAllMeatTypes(String queryOtherHosts) {

        var totalRevenueByMeatTypeStore = orderStoreService.ordersRevenuePerMeatTypeStore(REVENUE_TOTAL_PER_MEAT_TYPE);
        var totalRevenueByMeatType = totalRevenueByMeatTypeStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(totalRevenueByMeatType, 0);

        var revenueFromCurrentInstance = (ArrayList<OrderRevenueMeatTypeDTO>) StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue -> new OrderRevenueMeatTypeDTO(stringLongKeyValue.key, new OrderRevenue(stringLongKeyValue.value.getOrderCount(), stringLongKeyValue.value.getRunningRevenue())))
                .collect(Collectors.toList());

        if(Boolean.parseBoolean(queryOtherHosts)){
          var revenueFromOtherInstances = retrieveRevenueByMeatTypeFromOtherInstances();

          return (ArrayList<OrderRevenueMeatTypeDTO>) Stream.of(revenueFromCurrentInstance,revenueFromOtherInstances)
                  .filter(Objects::nonNull)
                  .flatMap(Collection::stream)
                  .collect(Collectors.toList());

        }

        return revenueFromCurrentInstance;
    }

    public OrderRevenueMeatTypeDTO getRevenueForOneMeatType(String meatType) {

        var keyHostInfoDTO = metaDataService.getStreamsMetaDataByKey(REVENUE_TOTAL_PER_MEAT_TYPE,meatType);

        if(Objects.nonNull(keyHostInfoDTO)){
            if(keyHostInfoDTO.getPort()==currentInstancePort){
                log.info("RETRIEVING DATA FROM CURRENT INSTANCE...");
                var totalRevenueByMeatTypeStore = orderStoreService.ordersRevenuePerMeatTypeStore(REVENUE_TOTAL_PER_MEAT_TYPE);
                var totalRevenueByMeatType = totalRevenueByMeatTypeStore.get(meatType);
                return new OrderRevenueMeatTypeDTO(meatType, new OrderRevenue(totalRevenueByMeatType.getOrderCount(), totalRevenueByMeatType.getRunningRevenue()));
            }
            else{
                log.info("RETRIEVING DATA FROM REMOTE INSTANCE...");
                return orderServiceClient.getAllRevenueForOneMeatType(keyHostInfoDTO,meatType);
            }

        }

        return null;
    }

    public List<RevenuePerStorePerMeatTypeDTO> getRevenueForAllMeatTypesByStore(String storeCode, String queryOtherHosts) {

        return getRevenueForAllStoresForAllMeatTypes(queryOtherHosts)
                .stream()
                .filter(revenuePerStorePerMeatTypeDTO -> revenuePerStorePerMeatTypeDTO.getStoreCode().equalsIgnoreCase(storeCode))
                .collect(Collectors.toList());


    }
    public List<RevenuePerStorePerMeatTypeDTO> getRevenueForAllStoresByMeatType(String meatType, String queryOtherHosts) {

                return getRevenueForAllStoresForAllMeatTypes(queryOtherHosts)
                        .stream()
                        .filter(revenuePerStorePerMeatTypeDTO -> revenuePerStorePerMeatTypeDTO.getMeatType().equalsIgnoreCase(meatType))
                        .collect(Collectors.toList());
    }

    public ArrayList<RevenuePerStorePerMeatTypeDTO> getRevenueForAllStoresForAllMeatTypes(String queryOtherHosts) {

        var totalRevenueByStoreStore = orderStoreService.ordersRevenuePerStorePerMeatTypeStore(REVENUE_TOTAL_PER_MEAT_TYPE_PER_STORE);
        var totalRevenueByStore = totalRevenueByStoreStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(totalRevenueByStore, 0);

        var revenuePerStorePerMeatTypeCurrentInstance = (ArrayList<RevenuePerStorePerMeatTypeDTO>) StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue ->
                        new RevenuePerStorePerMeatTypeDTO(stringLongKeyValue.key.substring(0, stringLongKeyValue.key.indexOf('-')),
                                stringLongKeyValue.key.substring(stringLongKeyValue.key.indexOf('-') + 1),
                                new OrderRevenue(stringLongKeyValue.value.getOrdersCount(), stringLongKeyValue.value.getRunningRevenue())))
                .collect(Collectors.toList());

        if(Boolean.parseBoolean(queryOtherHosts)){
            var revenuePerStorePerMeatTypeOtherInstances = getRevenueForAllStoresForAllMeatTypesOtherInstances();

            return (ArrayList<RevenuePerStorePerMeatTypeDTO>) Stream.of(revenuePerStorePerMeatTypeCurrentInstance,
                            revenuePerStorePerMeatTypeOtherInstances)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
        return revenuePerStorePerMeatTypeCurrentInstance;
    }

    private ArrayList<RevenuePerStorePerMeatTypeDTO> getRevenueForAllStoresForAllMeatTypesOtherInstances(){
        var otherKafkaStreamsHostInstances = otherHosts();
        log.info("otherHosts: {} ", otherKafkaStreamsHostInstances);

        if(!otherKafkaStreamsHostInstances.isEmpty()){
            return (ArrayList<RevenuePerStorePerMeatTypeDTO>) otherHosts()
                    .stream()
                    .map(hostInfoDTO -> orderServiceClient.getRevenueForAllStoresForAllMeatTypes(hostInfoDTO,"",""))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
        return null;
    }
}
