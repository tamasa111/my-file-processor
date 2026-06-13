package org.rayshan.processor.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.Data;

import java.util.List;

@Data
@RegisterForReflection
public class WhitelistResponse {

    @JsonProperty("statusCode")
    private int statusCode;

    @JsonProperty("errorMsg")
    private String errorMsg;

    @JsonProperty("data")
    private List<WhitelistData> data;
}