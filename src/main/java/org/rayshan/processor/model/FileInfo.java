package org.rayshan.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class FileInfo {
    private String correlationId;
    private String fileName;
    private String datasource;
    private String receivedAt;
    private String ingestedS3Bucket;
    private String ingestedS3Key;
    private Long fileSizeBytes;
    private String checksum;
    private String delimiter;
    private boolean fileTransform;
    private Map<String,Object> fileTransformConfig;
    private List<String> wlFilterTypes;


}
