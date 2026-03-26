package org.sunbird.peervalidation.service;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.IndexerService;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.logger.CbExtLogger;
import org.sunbird.core.producer.Producer;
import org.sunbird.storage.service.StorageServiceImpl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class PeerValidationServiceImpl implements PeerValidationService {

    private final CbExtLogger logger = new CbExtLogger(getClass().getName());

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private Producer kafkaProducer;

    @Autowired
    private AccessTokenValidator accessTokenValidator;

    @Autowired
    private CbExtServerProperties serverProperties;

    @Autowired
    private IndexerService indexerService;

    @Autowired
    private StorageServiceImpl storageServiceImpl;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    @Override
    public SBApiResponse initReportDownload(String formId, String authUserToken) {
        SBApiResponse response = new SBApiResponse();

        try {
            if (StringUtils.isEmpty(formId)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_FORM_ID_REQUIRED, Constants.FAILED);
            }
            if (StringUtils.isEmpty(authUserToken)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_AUTH_TOKEN_REQUIRED, Constants.FAILED);
            }

            // Extract userId from token
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
            if (StringUtils.isEmpty(userId)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_INVALID_TOKEN, Constants.FAILED);
            }

            String rootOrgId = fetchRootOrgIdFromUserId(userId);
            if (StringUtils.isEmpty(rootOrgId)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_FETCH_ROOT_ORG_FAILED + userId, Constants.FAILED);
            }

            List<String> userRoles = storageServiceImpl.getUserRoles(userId, rootOrgId);

            // Allow access if user has either SPV ADMIN or MDO ADMIN or MDO LEADER role
            if (!userRoles.contains(Constants.MDO_ADMIN) && !userRoles.contains(Constants.MDO_LEADER) && !userRoles.contains(Constants.SPV_ADMIN)) {
                logger.info("User " + userId + " does not have required role");
                return setCommonResponse(response, HttpStatus.FORBIDDEN, "Forbidden: User does not have required role", Constants.FAILED);
            }

            String existingReportCheck = checkExistingReportWithin24Hours(rootOrgId, formId);
            if (!StringUtils.isEmpty(existingReportCheck)) {
                return setCommonResponse(response, HttpStatus.CONFLICT, existingReportCheck, Constants.FAILED);
            }

            // Validate formId exists in Elasticsearch with the given rootOrgId and contextType
            Map<String, Object> formData = getFormData(formId, rootOrgId);
            if (MapUtils.isEmpty(formData)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_FORM_NOT_FOUND, Constants.FAILED);
            }

            // Extract form metadata
            String formTitle = (String) formData.get(Constants.TITLE);
            String thumbnail = null;
            String orgName = null;

            // Extract thumbnail from additionalProperties
            Map<String, Object> additionalProperties = (Map<String, Object>) formData.get(Constants.ADDITIONAL_PROPERTIES);
            if (additionalProperties != null) {
                thumbnail = (String) additionalProperties.get(Constants.THUMBNAIL_LOWER);
            }

            // Extract orgName from createdFor array
            List<Map<String, Object>> createdFor = (List<Map<String, Object>>) formData.get(Constants.CREATED_FOR);
            if (createdFor != null && !createdFor.isEmpty()) {
                orgName = (String) createdFor.get(0).get(Constants.ORG_NAME);
            }

            String identifier = UUIDs.timeBased().toString();
            Date now = new Timestamp(System.currentTimeMillis());

            Map<String, Object> record = new HashMap<>();
            record.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            record.put(Constants.FORM_ID_LOWER, formId);
            record.put(Constants.IDENTIFIER, identifier);
            record.put(Constants.REQUESTED_BY, userId);
            record.put(Constants.STATUS, Constants.REPORT_STATUS_IN_PROGRESS);
            record.put(Constants.DATE_CREATED_ON_CASSANDRA, now);
            record.put(Constants.DATE_UPDATED_ON_CASSANDRA, now);
            record.put(Constants.CREATED_BY, userId);
            record.put(Constants.UPDATED_BY, userId);
            record.put(Constants.TOTAL_RECORDS_CASSANDRA, 0);
            record.put(Constants.SUCCESSFUL_RECORDS_COUNT, 0);
            record.put(Constants.FAILED_RECORDS_COUNT, 0);
            record.put(Constants.FORM_TITLE_LOWER, formTitle);
            record.put(Constants.THUMBNAIL_LOWER, thumbnail);
            record.put(Constants.ORG_NAME_LOWER, orgName);

            logger.info("Inserting report request into Cassandra with TTL");

            SBApiResponse cassandraResponse = cassandraOperation.insertRecordWithTTL(
                    Constants.KEYSPACE_SUNBIRD, Constants.USER_SURVEY_REPORT, record, serverProperties.getPeerValidationReportTtlSeconds());

            if (!Constants.SUCCESS.equalsIgnoreCase((String) cassandraResponse.getResult().get("response"))) {
                logger.info("Failed to insert download request into Cassandra for formId: " + formId + ", userId: " + userId);
                return setCommonResponse(response, HttpStatus.INTERNAL_SERVER_ERROR, Constants.ERR_MSG_CREATE_REQUEST_FAILED, Constants.FAILED);
            }

            // Publish Kafka message
            Map<String, Object> requestMap = new HashMap<>();
            requestMap.put(Constants.ROOT_ORG_ID, rootOrgId);
            requestMap.put(Constants.FORM_ID, formId);
            requestMap.put(Constants.IDENTIFIER, identifier);
            requestMap.put(Constants.REQUESTED_BY_CAMEL, userId);
            requestMap.put(Constants.CREATED_ON, dateFormat.format(now));

            kafkaProducer.push(serverProperties.getReportDownloadRequestsTopic(), requestMap);
            logger.info("Published Kafka message for download request. Identifier: " + identifier + ", FormId: " + formId);

            Map<String, Object> responseData = new HashMap<>();
            responseData.put(Constants.IDENTIFIER, identifier);
            responseData.put(Constants.FORM_ID, formId);
            responseData.put(Constants.STATUS, Constants.REPORT_STATUS_IN_PROGRESS);
            responseData.put(Constants.MESSAGE, Constants.MSG_REQUEST_INITIATED);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.put(Constants.RESPONSE, responseData);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Exception in initReportDownload for formId: " + formId, e);
            return setCommonResponse(response, HttpStatus.INTERNAL_SERVER_ERROR,
                    Constants.ERR_MSG_INITIATE_REQUEST_FAILED + e.getMessage(), Constants.FAILED);
        }
        return response;
    }

    @Override
    public SBApiResponse listReportDownloads(String authUserToken) {
        SBApiResponse response = new SBApiResponse();
        String rootOrgId = null;
        try {
            if (StringUtils.isEmpty(authUserToken)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_AUTH_TOKEN_REQUIRED, Constants.FAILED);
            }

            // Extract userId from token
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
            if (StringUtils.isEmpty(userId)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_INVALID_TOKEN, Constants.FAILED);
            }

            // Fetch rootOrgId from user table
            rootOrgId = fetchRootOrgIdFromUserId(userId);
            if (StringUtils.isEmpty(rootOrgId)) {
                return setCommonResponse(response, HttpStatus.BAD_REQUEST, Constants.ERR_MSG_FETCH_ROOT_ORG_FAILED + userId, Constants.FAILED);
            }

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);

            List<String> fields = new ArrayList<>();
            List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.USER_SURVEY_REPORT, propertyMap, fields);

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.put(Constants.RESPONSE, records != null ? records : new ArrayList<>());
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Exception in listReportDownloads for orgId: " + rootOrgId, e);
            return setCommonResponse(response, HttpStatus.INTERNAL_SERVER_ERROR,
                    Constants.ERR_MSG_FETCH_REQUESTS_FAILED + e.getMessage(), Constants.FAILED);
        }
        return response;
    }

    /**
     * Fetch rootOrgId from user table using userId
     */
    private String fetchRootOrgIdFromUserId(String userId) {
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);

            List<String> fields = new ArrayList<>();
            fields.add(Constants.ROOT_ORG_ID);

            List<Map<String, Object>> userRecords = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap, fields);

            if (userRecords != null && !userRecords.isEmpty()) {
                return (String) userRecords.get(0).get(Constants.ROOT_ORG_ID);
            }
        } catch (Exception e) {
            logger.error("Error fetching rootOrgId for userId: " + userId, e);
        }
        return null;
    }

    /**
     * Common method to build response
     */
    private SBApiResponse setCommonResponse(SBApiResponse response, HttpStatus statusCode, String errorMessage, String status) {
        response.setResponseCode(statusCode);
        response.getParams().setErrmsg(errorMessage);
        response.getParams().setStatus(status);
        return response;
    }

    /**
     * Fetch form data from Elasticsearch with the given rootOrgId and contextType
     */
    private Map<String, Object> getFormData(String formId, String rootOrgId) {
        try {
            // Build the Elasticsearch query
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            boolQuery.must(QueryBuilders.termQuery(Constants.FORM_ID, formId));
            boolQuery.must(QueryBuilders.termQuery(Constants.CONTEXT_TYPE, Constants.CONTEXT_TYPE_PEER_VALIDATION_SURVEY));

            // Query for nested field createdFor.orgId (without .keyword suffix)
            BoolQueryBuilder nestedQuery = QueryBuilders.boolQuery();
            nestedQuery.must(QueryBuilders.termQuery(Constants.CREATED_FOR + Constants.DOT_SEPARATOR + Constants.ORG_ID, rootOrgId));
            boolQuery.must(QueryBuilders.nestedQuery(Constants.CREATED_FOR, nestedQuery, org.apache.lucene.search.join.ScoreMode.None));

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(boolQuery);
            searchSourceBuilder.size(1); // We only need one document

            // Log the query for debugging
            logger.info("Elasticsearch query for formId validation: " + searchSourceBuilder);

            SearchResponse searchResponse = indexerService.getEsResult(
                    serverProperties.getFormMetaDataIndex(),
                    Constants.ES_DOC_TYPE,
                    searchSourceBuilder,
                    ProjectUtil.ESIndexType.IGOT_ES
            );

            if (searchResponse != null && searchResponse.getHits().getTotalHits() > 0) {
                logger.info("Elasticsearch query returned " + searchResponse.getHits().getTotalHits() + " hits for formId: " + formId + ", rootOrgId: " + rootOrgId);
                return searchResponse.getHits().getHits()[0].getSourceAsMap();
            } else {
                logger.warn("Elasticsearch query returned null or no hits for formId: " + formId + ", rootOrgId: " + rootOrgId);
                return null;
            }
        } catch (Exception e) {
            logger.error("Error fetching form data for formId: " + formId + " and rootOrgId: " + rootOrgId, e);
            return null;
        }
    }

    /**
     * Check if there's already a report generated within last 24 hours for the given form
     * Returns error message if a recent report exists, null otherwise
     */
    private String checkExistingReportWithin24Hours(String rootOrgId, String formId) {
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            propertyMap.put(Constants.FORM_ID_LOWER, formId);

            List<String> fields = Arrays.asList(Constants.STATUS, Constants.DATE_CREATED_ON_CASSANDRA);

            List<Map<String, Object>> existingRecords = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.USER_SURVEY_REPORT, propertyMap, fields);

            if (!CollectionUtils.isEmpty(existingRecords)) {
                long currentTime = System.currentTimeMillis();
                long reportRestrictionMillis = serverProperties.getPeerValidationReportRestrictionHours() * Constants.HOURS_TO_MILLISECONDS;
                long inprogressRestrictionMillis = serverProperties.getPeerValidationReportInprogressRestrictionHours() * Constants.HOURS_TO_MILLISECONDS;

                for (Map<String, Object> record : existingRecords) {
                    String status = (String) record.get(Constants.STATUS);
                    Date createdOn = (Date) record.get(Constants.DATE_CREATED_ON);

                    if (createdOn == null) {
                        continue;
                    }

                    long timeSinceCreation = currentTime - createdOn.getTime();

                    // Check if report was completed and created within configured hours
                    if (Constants.REPORT_STATUS_COMPLETED.equalsIgnoreCase(status) && timeSinceCreation < reportRestrictionMillis) {
                        logger.info("Report already generated within " + serverProperties.getPeerValidationReportRestrictionHours() + " hours for formId: " + formId + ", rootOrgId: " + rootOrgId);
                        return "A report for this form was already generated within the last 24 hours. Please try again later.";
                    }

                    // Check if there's an in-progress request within configured hours
                    if (Constants.REPORT_STATUS_IN_PROGRESS.equalsIgnoreCase(status) && timeSinceCreation < inprogressRestrictionMillis) {
                        logger.info("Report generation already in progress for formId: " + formId + ", rootOrgId: " + rootOrgId);
                        return "A report generation is already in progress for this form. Please wait for it to complete.";
                    }
                }
            }
            return null;
        } catch (Exception e) {
            logger.info("Error checking existing reports for formId: " + formId + ", rootOrgId: " + rootOrgId + ": " + e);
            return null;
        }
    }
}
