package org.rayshan.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class CustomerInfo {

    private String customerId;
    private String customerName;
    private boolean wlFilter;
    private int[] wlIndexes;
    private WLFilterConfig wlFilterConfig;
    private List<CustomerDestination> customerDestinations;
}
