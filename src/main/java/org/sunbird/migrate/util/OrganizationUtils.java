package org.sunbird.migrate.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class OrganizationUtils {

    private final CbExtServerProperties serverConfig;

    @Autowired
    public OrganizationUtils(CbExtServerProperties serverConfig) {
        this.serverConfig = serverConfig;
    }

    public Map<String, Object> generateOrgSearchRequest(String orgId, List<String> fields, Map<String, Object> extraFilters) {
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.STATUS, serverConfig.getStatus());
        filters.put(Constants.MINISTRY_OR_STATE_ID, orgId);

        if (extraFilters != null && !extraFilters.isEmpty()) {
            filters.putAll(extraFilters);
        }

        Map<String, Object> innerRequest = new HashMap<>();
        innerRequest.put(Constants.FILTERS, filters);
        innerRequest.put(Constants.FIELDS_CONSTANT,
                (fields != null && !fields.isEmpty()) ? fields : Arrays.asList(Constants.CHANNEL, Constants.ID));
        innerRequest.put(Constants.LIMIT, serverConfig.getOrgSearchLimit());
        innerRequest.put(Constants.OFFSET, 0);

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, innerRequest);

        return request;
    }

}
