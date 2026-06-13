package org.rayshan.processor.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.Data;

import java.util.List;

@Data
@RegisterForReflection
public class WhitelistData {

    @JsonProperty("systemName")
    private String systemName;

    @JsonProperty("wlCategory")
    private String wlCategory;

    @JsonProperty("values")
    private List<String> values;
}