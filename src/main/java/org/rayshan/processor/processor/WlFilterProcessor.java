package org.rayshan.processor.processor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.rayshan.processor.model.CustomerInfo;
import org.rayshan.processor.model.WhitelistData;
import org.rayshan.processor.service.WhitelistClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@ApplicationScoped
public class WlFilterProcessor implements Processor {
    private final Logger LOG = LoggerFactory.getLogger(WlFilterProcessor.class);

    @Inject
    WhitelistClientService whitelistClientService;

    @Override
    public void process(Exchange exchange) throws Exception {
        LOG.info("-----> Calling whitelist in processor. exchange: {}", exchange);
        CustomerInfo customerInfo =
                exchange.getProperty("customerInfo", CustomerInfo.class);
        LOG.info("Customer Info: {}", customerInfo);
        List<WhitelistData> data = whitelistClientService.getFullWL();
        LOG.info("WL response = {}", data);
    }
}
