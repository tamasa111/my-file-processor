package org.rayshan.processor.service;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

@ApplicationScoped
public class StartupService {

    private static final Logger LOGGER = Logger.getLogger(StartupService.class);

    void onStart(@Observes StartupEvent evt) {
        LOGGER.info("=========> my-processor application started successfully!");
    }
}
