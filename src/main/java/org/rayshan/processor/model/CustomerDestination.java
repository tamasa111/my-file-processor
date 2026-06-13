package org.rayshan.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class CustomerDestination {
    private String destinationId;
    private boolean active;
    private String processedS3Bucket;
    private String processedS3Key;
}
