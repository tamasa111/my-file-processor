package org.rayshan.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class WLFilterConfig {

    private List<String> wlListType;
    private List<String> wlFilterType;
}
