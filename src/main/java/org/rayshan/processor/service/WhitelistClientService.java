package org.rayshan.processor.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.rayshan.processor.client.WhitelistClient;
import org.rayshan.processor.config.AppConfig;
import org.rayshan.processor.model.WhitelistData;
import org.rayshan.processor.model.WhitelistResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@ApplicationScoped
public class WhitelistClientService {
    private final Logger LOG = LoggerFactory.getLogger(WhitelistClientService.class);

    @Inject
    AppConfig appConfig;

    @RestClient
    WhitelistClient wlApiClient;

    public List<WhitelistData> getFullWL() {
//        String url = appConfig.whitelistService().service() + "/whitelistService/whitelist/getFullWL";
//        LOG.info("====> Getting url {}", url);
//        APIResponse dummyResp = new APIResponse();
//        List<GroupedRecord> data = new ArrayList<>();
//        GroupedRecord rec1 = new GroupedRecord("TEST", "TEST-GRP", new ArrayList<>());
//        rec1.getValues().add("12345");
//        data.add(rec1);
//        dummyResp.setData(data);
//        LOG.info("---------- DUMMY object for class loading ----- : {}", dummyResp);
        //String auth = appConfig.whitelistService().username() + ":" + appConfig.whitelistService().password();
        //String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        LOG.info("====> Calling WL service");
        WhitelistResponse resp = wlApiClient.getSystemWhitelist();
        LOG.info("====> resp : {}", resp);
        return resp.getData();
    }
}
