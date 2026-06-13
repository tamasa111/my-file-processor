package org.rayshan.processor.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.rayshan.processor.client.WhitelistClient;
import org.rayshan.processor.model.WhitelistResponse;

import java.time.LocalDateTime;
import java.util.Map;

@Path("/api")
public class StatusResource {

    private static final Logger LOGGER = Logger.getLogger(StatusResource.class);

    @Inject
    @RestClient
    WhitelistClient whitelistClient;

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getStatus() {
        LOGGER.info("GET /api/status called");
        return Map.of(
            "status",    "UP",
            "service",   "my-processor",
            "version",   "1.0.0-SNAPSHOT",
            "timestamp", LocalDateTime.now().toString()
        );
    }

    @GET
    @Path("/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public WhitelistResponse hello() {
        LOGGER.info("GET /api/hello called - fetching whitelist from external service");
        return whitelistClient.getSystemWhitelist();
    }
}
