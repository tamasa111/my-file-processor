package org.rayshan.processor.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Data;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.rayshan.processor.model.CustomerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class KafkaMessageProcessor implements Processor {
    private final Logger log = LoggerFactory.getLogger(KafkaMessageProcessor.class);
    @Inject
    ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        String fileName = exchange.getProperty("fileName", String.class);
        String outputTopic = exchange.getProperty("outputTopic", String.class);
        String correlationId = exchange.getProperty("correlationId", String.class);

        log.info("Built Kafka message for file {}", fileName);
        log.info("Built Kafka message for output topic {}", outputTopic);

        CustomerInfo customerInfo =
                exchange.getProperty("customerInfo", CustomerInfo.class);
        log.info("KafkaMessageProcessor - Customer Info: {}", customerInfo);

        Message msg1 = new Message();
        msg1.setKey("filename");
        msg1.setValue(fileName);

        msg1.setCorrelationId(correlationId);
        msg1.setCustomerId(customerInfo.getCustomerId());
        msg1.setFileName(fileName);

        // Set JSON body and use filename as Kafka partition key
        exchange.getIn().setBody(objectMapper.writeValueAsString(msg1));
        exchange.getIn().setHeader("kafka.KEY", fileName);
    }

    @Data
    private static class Message {
        private String key;
        private String value;

        private String correlationId;
        private String customerId;
        private String bucketName;
        private String fileName;
    }
}
