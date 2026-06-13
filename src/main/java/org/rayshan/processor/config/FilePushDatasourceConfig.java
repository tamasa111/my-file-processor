package org.rayshan.processor.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class FilePushDatasourceConfig {
    @ConfigProperty(name = "filepush.datasource.name")
    String datasource;

    public String getDatasource(){
        return datasource;
    }

}
