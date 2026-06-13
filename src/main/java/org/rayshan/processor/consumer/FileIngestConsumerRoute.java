package org.rayshan.processor.consumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.rayshan.processor.config.AppConfig;
import org.rayshan.processor.config.FilePushDatasourceConfig;
import org.rayshan.processor.model.FileIngestedEvent;

@ApplicationScoped
public class FileIngestConsumerRoute extends RouteBuilder {
    @Inject
    AppConfig appConfig;

    @Inject
    FilePushDatasourceConfig datasourceConfig;

    @Override
    public void configure() throws Exception {
        String datasource = datasourceConfig.getDatasource();
        String topic = "file.ingested." + datasource;
        String groupId = datasource + "-process-group";

        onException(Exception.class)
                .log("Failed to process file - ${exchangeProperty.fileName} -- correlationId - ${exchangeProperty.correlationId} ingested event  : ${exception.message}")
                .handled(true);

        from("kafka:" + topic
                + "?brokers={{camel.component.kafka.brokers}}"
                + "&groupId=" + groupId
                + "&autoOffsetReset=" + appConfig.kafka().offsetReset()
                + "&autoCommitEnable=false"
                + "&allowManualCommit=true"
                + "&consumersCount=5"
                + "&valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer")
                .unmarshal().json(JsonLibrary.Jackson, FileIngestedEvent.class)
                .process(exchange -> {
                    FileIngestedEvent event = exchange.getIn().getBody(FileIngestedEvent.class);
                    exchange.setProperty("correlationId", event.getFile().getCorrelationId());
                    exchange.setProperty("fileName", event.getFile().getFileName());

                })
                .log("Received- file - ${exchangeProperty.fileName} -- correlationId - ${exchangeProperty.correlationId}")

                .setHeader("datasource",constant(datasource))
                .to("direct:process-ingested-file")
                .process(exchange -> {
                    KafkaManualCommit manual = exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
                    if(manual !=  null) {
                        manual.commit();
                        log.info("Offset commited successfully");

                    }
                })
                .log("File Ingested and processed successfully");



    }
}
