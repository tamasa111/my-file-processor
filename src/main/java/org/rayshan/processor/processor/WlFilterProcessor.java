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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class WlFilterProcessor implements Processor {
    private final Logger LOG = LoggerFactory.getLogger(WlFilterProcessor.class);

    @Inject
    WhitelistClientService whitelistClientService;

    private final Map<String, Set<String>> whitelistCache = new ConcurrentHashMap<>();
    private volatile boolean whitelistLoaded = false;

    @Override
    public void process(Exchange exchange) throws Exception {
        CustomerInfo customerInfo = exchange.getProperty("customerInfo", CustomerInfo.class);
        String record = exchange.getIn().getBody(String.class);

        if (record == null || record.trim().isEmpty()) {
            exchange.getIn().setBody(null);
            return;
        }

        ensureWhitelistLoaded(customerInfo);

        String filterColumn = getFilterColumn(customerInfo);
        String delimiter = exchange.getProperty("csvDelimiter", String.class);
        if (delimiter == null) {
            delimiter = ",";
        }

        String fieldValue = extractCsvField(record, filterColumn, delimiter, exchange);

        boolean inWhitelist = fieldValue != null && isInWhitelist(customerInfo, fieldValue);
        String filterType = getFilterType(customerInfo);
        
        boolean shouldKeep;
        if ("SELECT_WL_VALUES_ONLY".equals(filterType)) {
            shouldKeep = inWhitelist;
        } else {
            shouldKeep = !inWhitelist;
        }
        
        if (shouldKeep) {
            exchange.getIn().setBody(record);
        } else {
            LOG.debug("Record filtered out (in whitelist): {}", fieldValue);
            exchange.getIn().setBody(null);
        }
    }

    private void ensureWhitelistLoaded(CustomerInfo customerInfo) {
        if (whitelistLoaded) {
            return;
        }
        synchronized (this) {
            if (whitelistLoaded) {
                return;
            }
            List<WhitelistData> wlData = whitelistClientService.getFullWL();
            for (WhitelistData data : wlData) {
                if (data.getWlCategory() != null && data.getValues() != null) {
                    whitelistCache.computeIfAbsent(data.getWlCategory(), k -> ConcurrentHashMap.newKeySet())
                            .addAll(data.getValues());
                }
            }
            whitelistLoaded = true;
            LOG.info("Whitelist loaded with {} categories", whitelistCache.size());
        }
    }

    private String getFilterColumn(CustomerInfo customerInfo) {
        if (customerInfo.getWlFilterConfig() != null
                && customerInfo.getWlFilterConfig().getWlListType() != null
                && !customerInfo.getWlFilterConfig().getWlListType().isEmpty()) {
            return customerInfo.getWlFilterConfig().getWlListType().get(0);
        }
        return "id";
    }

    private String extractCsvField(String record, String filterColumn, String delimiter, Exchange exchange) {
        List<String> headers = exchange.getProperty("csvHeaders", List.class);
        if (headers == null) {
            return null;
        }

        int columnIndex = headers.indexOf(filterColumn);
        if (columnIndex < 0) {
            LOG.warn("Filter column '{}' not found in CSV headers: {}", filterColumn, headers);
            return null;
        }

        String[] fields = record.split(delimiter, -1);
        if (columnIndex >= fields.length) {
            return null;
        }
        return fields[columnIndex].trim();
    }

    private String getFilterType(CustomerInfo customerInfo) {
        if (customerInfo.getWlFilterConfig() != null
                && customerInfo.getWlFilterConfig().getFilterType() != null) {
            return customerInfo.getWlFilterConfig().getFilterType();
        }
        return "REMOVE_WL_VALUES";
    }

    private boolean isInWhitelist(CustomerInfo customerInfo, String fieldValue) {
        if (customerInfo.getWlFilterConfig() == null
                || customerInfo.getWlFilterConfig().getWlListType() == null) {
            return false;
        }

        for (String listType : customerInfo.getWlFilterConfig().getWlListType()) {
            Set<String> values = whitelistCache.get(listType);
            if (values != null && values.contains(fieldValue)) {
                return true;
            }
        }
        return false;
    }

    public void reloadWhitelist() {
        whitelistCache.clear();
        whitelistLoaded = false;
    }
}