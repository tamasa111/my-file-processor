package org.rayshan.processor.processor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.rayshan.processor.model.CustomerInfo;
import org.rayshan.processor.model.FileInfo;
import org.rayshan.processor.model.FileIngestedEvent;
import org.rayshan.processor.model.WhitelistData;
import org.rayshan.processor.service.WhitelistClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class CsvStreamFilterProcessor implements Processor {
    private final Logger LOG = LoggerFactory.getLogger(CsvStreamFilterProcessor.class);

    @Inject
    WhitelistClientService whitelistClientService;

    private final Map<String, Set<String>> whitelistCache = new ConcurrentHashMap<>();
    private volatile boolean whitelistLoaded = false;

    @Override
    public void process(Exchange exchange) throws Exception {
        CustomerInfo customerInfo = exchange.getProperty("customerInfo", CustomerInfo.class);
        FileIngestedEvent ingestedEvent = exchange.getProperty("ingestedEvent", FileIngestedEvent.class);
        FileInfo fileInfo = ingestedEvent.getFile();

        if (!customerInfo.isWlFilter()) {
            return;
        }

        String delimiter = exchange.getProperty("csvDelimiter", String.class);
        if (delimiter == null) {
            delimiter = ",";
        }
        final String finalDelimiter = delimiter;
        final CustomerInfo finalCustomerInfo = customerInfo;
        final Exchange finalExchange = exchange;

        ensureWhitelistLoaded(customerInfo);

        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        if (inputStream == null) {
            return;
        }

        // Use ByteArrayOutputStream to collect filtered content (provides known length for S3)
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                return;
            }

            String[] headerFields = headerLine.split(finalDelimiter, -1);
            int[] indexesToCheck = getIndexesToCheck(finalCustomerInfo, headerFields);

            writer.write(headerLine);
            writer.newLine();

            int keptCount = 0;
            int skippedCount = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] fields = line.split(finalDelimiter, -1);
                boolean inWhitelist = false;
                for (int idx : indexesToCheck) {
                    if (idx >= 0 && idx < fields.length) {
                        String fieldValue = fields[idx].trim();
                        if (isInWhitelist(finalCustomerInfo, fieldValue)) {
                            inWhitelist = true;
                            break;
                        }
                    }
                }

                String filterType = getFilterType(finalCustomerInfo);
                boolean shouldFilter;
                if ("SELECT_WL_VALUES_ONLY".equals(filterType)) {
                    shouldFilter = !inWhitelist;
                } else {
                    shouldFilter = inWhitelist;
                }

                if (!shouldFilter) {
                    writer.write(line);
                    writer.newLine();
                    keptCount++;
                } else {
                    skippedCount++;
                }
            }

            finalExchange.setProperty("filteredCount", keptCount);
            LOG.debug("Filtered {} lines, kept {} lines", skippedCount, keptCount);

        } catch (IOException e) {
            LOG.error("Error processing CSV stream", e);
            throw e;
        }

        // Set byte array directly - Camel handles content-length correctly for byte[]
        byte[] filteredBytes = outputStream.toByteArray();
        exchange.getIn().setBody(filteredBytes);
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

    private int[] getIndexesToCheck(CustomerInfo customerInfo, String[] headerFields) {
        if (customerInfo.getWlFilterConfig() != null
                && customerInfo.getWlFilterConfig().getWlIndexes() != null
                && customerInfo.getWlFilterConfig().getWlIndexes().length > 0) {
            return customerInfo.getWlFilterConfig().getWlIndexes();
        }
        // Fallback: find column index by name from wlFilterConfig
        String filterColumn = getFilterColumn(customerInfo);
        List<String> headers = Arrays.asList(headerFields);
        int columnIndex = headers.indexOf(filterColumn);
        if (columnIndex >= 0) {
            return new int[]{columnIndex};
        }
        return new int[0];
    }

    private String getFilterColumn(CustomerInfo customerInfo) {
        if (customerInfo.getWlFilterConfig() != null
                && customerInfo.getWlFilterConfig().getWlListType() != null
                && !customerInfo.getWlFilterConfig().getWlListType().isEmpty()) {
            return customerInfo.getWlFilterConfig().getWlListType().get(0);
        }
        return "id";
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