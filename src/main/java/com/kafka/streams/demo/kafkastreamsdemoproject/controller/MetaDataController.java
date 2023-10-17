package com.kafka.streams.demo.kafkastreamsdemoproject.controller;

import com.kafka.streams.demo.kafkastreamsdemoproject.domain.HostInfoDTO;
import com.kafka.streams.demo.kafkastreamsdemoproject.service.MetaDataService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/metadata")
public class MetaDataController {

    private final MetaDataService metaDataService;


    public MetaDataController(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }

    @GetMapping("/all/instances")
    public ResponseEntity<List<HostInfoDTO>> getStreamsMetaData(){
        return ResponseEntity.ok(metaDataService.getStreamsMetaData());
    }
}
