package org.rayshan.processor.processor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.aws2.s3.AWS2S3Constants;
import org.rayshan.processor.config.AppConfig;
import org.rayshan.processor.model.CustomerInfo;
import org.rayshan.processor.model.FileInfo;
import org.rayshan.processor.model.FileIngestedEvent;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class FilePushProcessingRoute extends RouteBuilder {
    @Inject
    AppConfig config;

    @ConfigProperty(name = "app.config.kafka.output-topic", defaultValue = "test-out-topic")
    String outputTopic;

    @Inject
    WlFilterProcessor wlFilterProcessor;

    @Inject
    KafkaMessageProcessor kafkaMessageProcessor;

    @Override
    public void configure() throws Exception {
        // Catch-all safety net
        onException(Exception.class)
                .maximumRedeliveries(1)
                .redeliveryDelay(2000)
                .handled(true)
                .log(LoggingLevel.ERROR,
                        "Unexpected failure - ${exchangeProperty.fileName} "
                                + "-- correlationId ${exchangeProperty.correlationId}",
                        "${exception.stacktrace}")
                .to("direct:handle-failed-file");

        //------ ENTRY POINT-----------------------//
        from("direct:process-ingested-file")
                .routeId("file-processsing-route")
                .log("Processing file - ${exchangeProperty.fileName} -- correlationId - ${exchangeProperty.correlationId} for datasource: ${header.datasource}")
                //split by customers
                .process(exchange -> {
                    FileIngestedEvent event = exchange.getIn().getBody(FileIngestedEvent.class);
                    exchange.setProperty("ingestedEvent", event);
                    exchange.getIn().setBody(event.getCustomers());
                })
                .split(body())
                .process(exchange -> {
                    CustomerInfo customerInfo = exchange.getIn().getBody(CustomerInfo.class);
                    FileIngestedEvent ingestedEvent = exchange.getProperty("ingestedEvent", FileIngestedEvent.class);
                    exchange.setProperty("customerInfo", customerInfo);
                    boolean directPublish = !ingestedEvent.getFile().isFileTransform() && !customerInfo.isWlFilter();
                    exchange.setProperty("directPublish", directPublish);
                })
                .choice()
                .when(exchangeProperty("directPublish").isEqualTo(true))
                .to("direct:publish-for-active-destination")
                .otherwise()
                .log("Processing required")
                .to("direct:full-pipeline")
                .end()
                .end()
        ;

        // ============ FULL PIPELINE ============
        from("direct:full-pipeline")
                .routeId("full-pipeline-route")
                // GUARANTEED cleanup - success or failure
                .onCompletion()
                .process(exchange -> {
                    Object body = exchange.getIn().getBody();
                    if (body instanceof java.io.Closeable c) {
                        try {
                            c.close();
                        } catch (Exception ignored) {
                        }
                    }
                })
                .log("Pipeline finished - {exchangeProperty.fileName}")
                .end()

                .log("Starting full pipeline - {exchangeProperty.fileName} "
                        + "-- correlationId - {exchangeProperty.correlationId}")

                // ----- Step 1: Read from S3 as a stream -----
                .setProperty("currentStage", constant("READ_FROM_S3"))
                .process(exchange -> {
                    FileIngestedEvent ingestedEvent =
                            exchange.getProperty("ingestedEvent", FileIngestedEvent.class);
                    FileInfo fileInfo = ingestedEvent.getFile();
                    exchange.getIn().setHeader(AWS2S3Constants.BUCKET_NAME, fileInfo.getIngestedS3Bucket());
                    exchange.getIn().setHeader(AWS2S3Constants.KEY, fileInfo.getIngestedS3Key());
                })
                // getObject -> body is a ResponseInputStream: true streaming, no full load
                //.to("aws2-s3://ignored?region=us-east-1&operation=getObject&autoCreateBucket=false&accessKey=7ZTpPkh9ShRuGxa7ShYMHlHEngSZm7lz&secretKey=xBacsX45mlf0N0PIt8g28QTW0t9hQ5-7&trustAllCertificates=true&overrideEndpoint=true&uriEndpointOverride=" + config.s3().endpoint())
                // forcePathStyle=true -> force path-style access
                .to(String.format("aws2-s3://file-push-test-src?region=%s&operation=getObject&autoCreateBucket=false&accessKey=%s&secretKey=%s&trustAllCertificates=true&overrideEndpoint=true&uriEndpointOverride=%s&forcePathStyle=true",
                        config.s3().region(),
                        config.s3().accessKey(),
                        config.s3().secretKey(),
                        config.s3().endpoint()))
                .log("Read ingest file from s3://${header.CamelAwsS3BucketName}/${header.CamelAwsS3Key}")

                // ----- Step 2: Apply transformation (stream -> stream) -----
                .setProperty("currentStage", constant("TRANSFORMATION"))
                .to("direct:apply-transformation")

                // ----- Step 3: Apply WL filter (stream -> stream) -----
                .setProperty("currentStage", constant("WL_FILTER"))
                .to("direct:apply-wl-filter")

                // ----- Step 4: Upload processed stream to S3 processed/ folder -----
                .setProperty("currentStage", constant("UPLOAD_TO_S3"))
                .process(exchange -> {
                    FileIngestedEvent ingestedEvent =
                            exchange.getProperty("ingestedEvent", FileIngestedEvent.class);
                    CustomerInfo customerInfo =
                            exchange.getProperty("customerInfo", CustomerInfo.class);
                    FileInfo fileInfo = ingestedEvent.getFile();
                    String processedKey =
                            "processed/" + customerInfo.getCustomerId() + "/" + fileInfo.getFileName();
                    exchange.getIn().setHeader(AWS2S3Constants.BUCKET_NAME, fileInfo.getIngestedS3Bucket());
                    exchange.getIn().setHeader(AWS2S3Constants.KEY, processedKey);
                    exchange.setProperty("processedS3Key", processedKey);
                    exchange.setProperty("processedS3Bucket", fileInfo.getIngestedS3Key());
                })
                // multiPartUpload=true -> uploads the InputStream in parts, no content length needed
                //.to("aws2-s3://ignored?region=us-east-1&autoCreateBucket=false&multiPartUpload=true&accessKey=7ZTpPkh9ShRuGxa7ShYMHlHEngSZm7lz&secretKey=xBacsX45mlf0N0PIt8g28QTW0t9hQ5-7&trustAllCertificates=true&overrideEndpoint=true&uriEndpointOverride=" + config.s3().endpoint())
                .to(String.format("aws2-s3://file-push-test-target?region=%s&autoCreateBucket=false&multiPartUpload=true&accessKey=%s&secretKey=%s&trustAllCertificates=true&overrideEndpoint=true&uriEndpointOverride=%s&forcePathStyle=true",
                        config.s3().region(),
                        config.s3().accessKey(),
                        config.s3().secretKey(),
                        config.s3().endpoint()))
                .log("Uploaded processed file to "
                        + "s3://${exchangeProperty.processedS3Bucket}/${exchangeProperty.processedS3Key}")

                // ----- Step 5: Hand off to Kafka (already implemented separately) -----
                .setProperty("currentStage", constant("SEND_TO_KAFKA"))
                .setProperty("outputTopic", constant(outputTopic))
                .to("direct:send-to-kafka")
                .setProperty("currentStage", constant("COMPLETE"))
                .log("Full pipeline complete - ${exchangeProperty.fileName} "
                        + "-- correlationId - ${exchangeProperty.correlationId}")
        ;

        // ============ TRANSFORMATION (stream -> stream, stub) ============
        from("direct:apply-transformation")
                .routeId("apply-transformation-route")
                .log("Applying transformation - ${exchangeProperty.fileName}")
                .choice()
                .when(simple("${exchangeProperty.ingestedEvent.file.fileTransform} == true"))
                // .bean("fileTransformationProcessor", "transform")
                .log("Transformation applied - ${exchangeProperty.fileName}")
                .otherwise()
                .log("No transformation required - ${exchangeProperty.fileName}")
                .end()
        ;

        // ============ WL FILTER (stream -> stream, stub) ============
        from("direct:apply-wl-filter")
                .routeId("apply-wl-filter-route")
                .log("Applying WL filter - ${exchangeProperty.fileName}")
                .choice()
                .when(simple("${exchangeProperty.customerInfo.wlFilter} == true"))
                .process(wlFilterProcessor)
                //.bean(WlFilterProcessor.class, "filter")
                .log("WL filter applied - ${exchangeProperty.fileName}")
                .otherwise()
                .log("WL filter not enabled for customer - skipping - ${exchangeProperty.fileName}")
                .end()
        ;

        from("direct:send-to-kafka")
                .routeId("send-to-kafka-route")
                .process(kafkaMessageProcessor)
                .setHeader("kafka.TOPIC", simple("${exchangeProperty.outputTopic}"))
                .log("Send ing message to kafka.")
                .to("kafka:${exchangeProperty.outputTopic}?brokers={{camel.component.kafka.brokers}}")
                .onException(Exception.class)
                    .handled(true)
                    .log(LoggingLevel.ERROR, "Failed to send to Kafka - ${exception.message}")
                    .end()
                .log("kafka message published for file: ${headerexchangeProperty.fileName}");

    }
}
