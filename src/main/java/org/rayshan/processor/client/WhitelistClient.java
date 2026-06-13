package org.rayshan.processor.client;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.rayshan.processor.model.WhitelistResponse;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/whitelistService/whitelist")
@RegisterRestClient(configKey = "whitelist-client")
@Produces(MediaType.APPLICATION_JSON)
@RegisterForReflection
public interface WhitelistClient {

    @GET
    @Path("/systemWlList")
    WhitelistResponse getSystemWhitelist();
}