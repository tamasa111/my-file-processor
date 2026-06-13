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
        String outputTopicPrefix = exchange.getProperty("outputTopicPrefix", String.class);
        String correlationId = exchange.getProperty("correlationId", String.class);

        log.info("Built Kafka message for file {}", fileName);
        log.info("Built Kafka message for output topic {}", outputTopic);

        CustomerInfo customerInfo =
                exchange.getProperty("customerInfo", CustomerInfo.class);
        log.info("KafkaMessageProcessor - Customer Info: {}", customerInfo);

        Message kafkaMessage = new Message();
        kafkaMessage.setKey("filename");
        kafkaMessage.setValue(fileName);

        kafkaMessage.setCorrelationId(correlationId);
        kafkaMessage.setFileName(fileName);

        if(customerInfo == null) {
            log.error("Customer Info is null");
        } else {
            kafkaMessage.setCustomerId(customerInfo.getCustomerId());
            exchange.setProperty("outputTopic", outputTopicPrefix + "." + customerInfo.getCustomerId());
        }

        // Set JSON body and use filename as Kafka partition key
        exchange.getIn().setBody(objectMapper.writeValueAsString(kafkaMessage));
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
