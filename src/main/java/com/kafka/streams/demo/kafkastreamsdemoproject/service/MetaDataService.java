package com.kafka.streams.demo.kafkastreamsdemoproject.service;

import com.kafka.streams.demo.kafkastreamsdemoproject.domain.HostInfoDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.domain.KeyHostInfoDTO;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class MetaDataService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    public MetaDataService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public List<HostInfoDTO> getStreamsMetaData(){
        return Objects.requireNonNull(streamsBuilderFactoryBean
                        .getKafkaStreams())
                .metadataForAllStreamsClients()
                .stream()
                .map(streamsMetadata -> {
                    var hostInfo = streamsMetadata.hostInfo();
                    return new HostInfoDTO(hostInfo.host(),hostInfo.port());
                })
                .collect(Collectors.toList());
    }

    public KeyHostInfoDTO getStreamsMetaDataByKey(String storeName, String key){
        var metaDataForKey = Objects.requireNonNull(streamsBuilderFactoryBean
                        .getKafkaStreams())
                .queryMetadataForKey(storeName,key, Serdes.String().serializer());

        if(Objects.nonNull(metaDataForKey)){
            var activeHost = metaDataForKey.activeHost();
            return new KeyHostInfoDTO(activeHost.host(), activeHost.port(), key);
        }

        return null;
    }

}
