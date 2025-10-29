package org.sunbird.org.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.util.*;

@Service
public class FrameworkUtil {


    @Autowired
    RedisCacheMgr redisCacheMgr;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandler;

    public void traverseByCategory(
            Sheet sheet,
            List<Map<String, Object>> categoryList,
            int level,
            Map<String, Object> node,
            List<String> path,
            int[] rowIndex
    ) {
        List<String> newPath = new ArrayList<>(path);
        newPath.add((String) node.get("name"));

        if (level == categoryList.size() - 1 || node.get("associations") == null) {
            while (newPath.size() < categoryList.size()) newPath.add("");
            Row row = sheet.createRow(rowIndex[0]++);
            for (int i = 0; i < categoryList.size(); i++) {
                row.createCell(i).setCellValue(newPath.get(i));
            }
            return;
        }

        List<Map<String, Object>> associations = (List<Map<String, Object>>) node.get("associations");
        if (associations != null && !associations.isEmpty()) {
            Map<String, Map<String, Object>> nextTermMap = new HashMap<>();
            List<Map<String, Object>> nextTerms = (List<Map<String, Object>>) categoryList.get(level + 1).get("terms");
            if (nextTerms != null) {
                for (Map<String, Object> t : nextTerms) {
                    nextTermMap.put((String) t.get("identifier"), t);
                }
            }
            for (Map<String, Object> assoc : associations) {
                Map<String, Object> nextNode = nextTermMap.get(assoc.get("identifier"));
                if (nextNode != null) {
                    traverseByCategory(sheet, categoryList, level + 1, nextNode, newPath, rowIndex);
                } else {
                    traverseByCategory(sheet, categoryList, level + 1, assoc, newPath, rowIndex);
                }
            }
        }
    }

    public void populateOrgDesignationMaster(Sheet sheet, String currentOrgId) throws Exception {
        List<Map<String, Object>> orgList = getMasterData(currentOrgId);
        int rowIndex = 1;
        if (CollectionUtils.isNotEmpty(orgList)) {
            for (Map<String, Object> org : orgList) {
                Row row = sheet.createRow(rowIndex++);

                String orgId = org.get("id") != null ? org.get("id").toString() : "";
                String orgName = org.get("orgName") != null ? org.get("orgName").toString() : "";

                row.createCell(0).setCellValue(orgId);

                row.createCell(1).setCellValue(orgName);

                row.createCell(2).setCellValue(orgName + Constants.LEFT_ANGLE_BRACKET + orgId + Constants.RIGHT_ANGLE_BRACKET);
            }
        } else {
            Row row = sheet.createRow(rowIndex++);
            row.createCell(0).setCellValue("No data found for the given organisation ID: " + currentOrgId);
        }
    }

    public List<Map<String, Object>> getMasterData(String orgId) throws Exception, InterruptedException {
        String masterDataOrg = redisCacheMgr.getCache(Constants.ORG_MASTER_DATA + "_" + orgId);
        if (StringUtils.isEmpty(masterDataOrg) || masterDataOrg.equals("[]") || masterDataOrg.equals("{}") || masterDataOrg.equalsIgnoreCase("null")) {
            List<Map<String, Object>> orgMasterData = populateDataFromApi(orgId);
            if (orgMasterData != null) {
                redisCacheMgr.putCache(Constants.ORG_MASTER_DATA + "_" + orgId, orgMasterData, serverProperties.getRedisMasterDataReadTimeOut());
                return orgMasterData;
            } else {
                return Collections.emptyList();
            }
        } else {
            return objectMapper.readValue(masterDataOrg, new TypeReference<List<Map<String, Object>>>() {
            });
        }
    }

    private List<Map<String, Object>> populateDataFromApi(String orgId) throws Exception {
        Thread.sleep(500);
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());
        String url = serverProperties.getLearnerServiceHost() + serverProperties.getOrgSearchUrl();
        Map<String, Object> termFrameworkCompetencies = (Map<String, Object>) outboundRequestHandler.fetchResultUsingPost(
                url, buildOrgSearchRequest(orgId), headers);
        if (MapUtils.isNotEmpty(termFrameworkCompetencies)) {
            Map<String, Object> result = ((Map<String, Object>) termFrameworkCompetencies.get(Constants.RESULT));
            if (MapUtils.isNotEmpty(result)) {
                Map<String, Object> frameworkObject = ((Map<String, Object>) result.get(Constants.RESPONSE));
                if (MapUtils.isNotEmpty(frameworkObject)) {
                    return (List<Map<String, Object>>) frameworkObject.get(Constants.CONTENT);
                }
            }
        }
        return null;
    }

    private Map<String, Object> buildOrgSearchRequest(String orgId) {
        Map<String, Object> request = new HashMap<>();

        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.STATUS, 1);
        filters.put(Constants.MINISTRY_OR_STATE_ID, orgId);

        Map<String, String> sortBy = new HashMap<>();
        sortBy.put(Constants.ORG_NAME, Constants.ASC_ORDER);

        List<String> fields = Arrays.asList(Constants.ORG_NAME, Constants.ID);

        Map<String, Object> innerRequest = new HashMap<>();
        innerRequest.put(Constants.FILTERS, filters);
        innerRequest.put(Constants.SORT_BY, sortBy);
        innerRequest.put(Constants.FIELDS_CONSTANT, fields);
        innerRequest.put(Constants.LIMIT, serverProperties.getOrgSearchLimit());
        innerRequest.put(Constants.OFFSET, 0);

        request.put(Constants.REQUEST, innerRequest);

        return request;
    }

    public List<Map<String, Object>> populateDataFromFrameworkTerm(String frameworkId) throws Exception {
        Thread.sleep(500);
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());
        String url = serverProperties.getKmBaseHost() + serverProperties.getKmFrameWorkPath() + "/" + frameworkId;
        Map<String, Object> termFrameworkCompetencies = (Map<String, Object>) outboundRequestHandler.fetchUsingGetWithHeaders(
                url, headers);
        if (MapUtils.isNotEmpty(termFrameworkCompetencies)) {
            Map<String, Object> result = ((Map<String, Object>) termFrameworkCompetencies.get(Constants.RESULT));
            if (MapUtils.isNotEmpty(result)) {
                Map<String, Object> frameworkObject = ((Map<String, Object>) result.get(Constants.FRAMEWORK));
                if (MapUtils.isNotEmpty(frameworkObject)) {
                    return (List<Map<String, Object>>) frameworkObject.get(Constants.CATEGORIES);
                }
            }
        }
        return null;
    }

}
