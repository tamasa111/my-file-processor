package org.rayshan.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.Data;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@RegisterForReflection
public class FileIngestedEvent {
    public FileIngestedEvent() {}

    private String eventId;
    private String eventType;
    private String eventVersion;
    private String publishedAt;
    private String publishedBy;
    private FileInfo file;
    private List<CustomerInfo> customers;
}
