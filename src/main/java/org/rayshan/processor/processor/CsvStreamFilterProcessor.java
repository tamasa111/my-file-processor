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

        String filterColumn = getFilterColumn(customerInfo);
        final String finalFilterColumn = filterColumn;
        final CustomerInfo finalCustomerInfo = customerInfo;
        final Exchange finalExchange = exchange;

        ensureWhitelistLoaded(customerInfo);

        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        if (inputStream == null) {
            return;
        }

        final PipedOutputStream pipedOut = new PipedOutputStream();
        final PipedInputStream pipedIn = new PipedInputStream(pipedOut);

        Thread writerThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(pipedOut, StandardCharsets.UTF_8))) {

                String headerLine = reader.readLine();
                LOG.info("==> Header line read: {}", headerLine);
                if (headerLine == null) {
                    return;
                }

                List<String> headers = Arrays.asList(headerLine.split(finalDelimiter, -1));
                int columnIndex = headers.indexOf(finalFilterColumn);

                writer.write(headerLine);
                writer.newLine();

                int keptCount = 0;
                int skippedCount = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }
                    LOG.info("==> Body line read: {}", line);

                    String[] fields = line.split(finalDelimiter, -1);
                    if (columnIndex >= 0 && columnIndex < fields.length) {
                        String fieldValue = fields[columnIndex].trim();
                        if (isInWhitelist(finalCustomerInfo, fieldValue)) {
                            skippedCount++;
                        } else {
                            writer.write(line);
                            writer.newLine();
                            keptCount++;
                        }
                    } else {
                        writer.write(line);
                        writer.newLine();
                        keptCount++;
                    }
                }

                finalExchange.setProperty("filteredCount", keptCount);
                LOG.debug("Filtered {} lines, kept {} lines", skippedCount, keptCount);

            } catch (IOException e) {
                LOG.error("Error processing CSV stream", e);
            } finally {
                try {
                    pipedOut.close();
                } catch (IOException ignored) {
                }
            }
        });

        writerThread.start();
        writerThread.join();

        exchange.getIn().setBody(pipedIn);
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
                && customerInfo.getWlFilterConfig().getWlFilterType() != null
                && !customerInfo.getWlFilterConfig().getWlFilterType().isEmpty()) {
            return customerInfo.getWlFilterConfig().getWlFilterType().get(0);
        }
        return "id";
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