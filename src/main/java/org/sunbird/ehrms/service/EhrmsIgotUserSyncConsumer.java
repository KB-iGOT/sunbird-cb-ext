package org.sunbird.ehrms.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class EhrmsIgotUserSyncConsumer {

    private final Logger log = LoggerFactory.getLogger(getClass().getName());

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    RestTemplate restTemplate;

    @Value("${ehrms.api.base.url}")
    private String ehrmsBaseUrl;

    @Value("${ehrms.api.user.profile.path}")
    private String ehrmsUserProfilePath;

    @Value("${ehrms.api.user.qualifications.path}")
    private String ehrmsUserQualificationPath;

    @Value("${ehrms.api.key}")
    private String ehrmsApiKey;


    private final AtomicInteger totalUsersCount = new AtomicInteger(0);
    private final AtomicInteger existingUsersCount = new AtomicInteger(0);
    private final AtomicInteger notFoundUsersCount = new AtomicInteger(0);
    private final AtomicInteger profileUpdateSuccessCount = new AtomicInteger(0);
    private final AtomicInteger profileUpdateFailedCount = new AtomicInteger(0);

    List<String> statesAndUTs = Arrays.asList("Andaman And Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh", "Chhattisgarh", "Delhi", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu And Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "The Dadra And Nagar Haveli And Daman And Diu", "Tripura", "Uttarakhand", "Uttar Pradesh", "West Bengal");

    @KafkaListener(topics = "${ehrms.user.data.sync.kafka.topic}", groupId = "${ehrms.user.data.sync.kafka.group}")
    private void initiateEhrmsIgotDataSync(ConsumerRecord<String, String> data) {
        log.info("EhrmsIgotUserSyncConsumer::processMessage.. started.");
        try {
            if (org.apache.commons.lang3.StringUtils.isBlank(data.value())) {
                log.error("Error in EhrmsIgotUserSyncConsumer: Invalid or empty Kafka message received");
                return;
            }
            log.info("Ehrms user sync initiated successfully for data: {}", data.value());
            processEhrmsIgotDataSync(data.value());

        } catch (Exception e) {
            log.error("Error while syncing ehrms user data: {}", data.value(), e);
        }
    }

    public void processEhrmsIgotDataSync(String inputData) throws IOException {
        log.info("EhrmsIgotUserSyncConsumer:: processEhrmsIgotDataSync: Started");
        long duration = 0;
        long startTime = System.currentTimeMillis();
        Map<String, Object> request = mapper.readValue(inputData, new TypeReference<Map<String, Object>>() {
        });
        String jobId = request.get(Constants.JOB_ID).toString();
        Object jobStartDateObj = request.get(Constants.JOB_START_DATE);
        Date jobStartDate;

        if (jobStartDateObj instanceof Long) {
            jobStartDate = new Date((Long) jobStartDateObj);
        } else if (jobStartDateObj instanceof String) {
            jobStartDate = javax.xml.bind.DatatypeConverter.parseDateTime((String) jobStartDateObj).getTime();
        } else if (jobStartDateObj instanceof Date) {
            jobStartDate = (Date) jobStartDateObj;
        } else {
            throw new IllegalArgumentException("Invalid type for job_start_date: " + jobStartDateObj.getClass());
        }
        try {
            String from = request.get(Constants.EHRMS_FROM_DATE).toString();
            String to = request.get(Constants.EHRMS_TO_DATE).toString();
            String empCsv = callEmpApi(from, to);
            String qualCsv = callQualificationApi(from, to);

            List<Map<String, Object>> employees = parseCsv(empCsv);
            List<Map<String, Object>> quals = parseCsv(qualCsv);
            Map<String, Map<String, Object>> merged = join(employees, quals);

            for (String empCode : merged.keySet()) {
                processUserEhrmsDataLine(merged.get(empCode));
            }
        } catch (Exception e) {
            log.error(String.format("Error in the scheduler to generate the BP report %s", e.getMessage()), e);
            updateDataBase(jobId, jobStartDate, Constants.FAILED_UPPERCASE, totalUsersCount.get(), existingUsersCount.get(), notFoundUsersCount.get(), profileUpdateSuccessCount.get(), profileUpdateFailedCount.get());
        }
        duration = System.currentTimeMillis() - startTime;
        log.info("EhrmsIgotUserSyncConsumer:: processEhrmsIgotDataSync: Completed. Time taken: " + duration + " milli-seconds");
        updateDataBase(jobId, jobStartDate, Constants.SUCCESS_UPPERCASE, totalUsersCount.get(), existingUsersCount.get(), notFoundUsersCount.get(), profileUpdateSuccessCount.get(), profileUpdateFailedCount.get());
    }

    private String callEmpApi(String from, String to) throws Exception {
        String url = ehrmsBaseUrl + ehrmsUserProfilePath;
        return callApi(url, from, to);
    }

    private String callQualificationApi(String from, String to) throws Exception {
        String url = ehrmsBaseUrl + ehrmsUserQualificationPath;
        return callApi(url, from, to);
    }

    private String callApi(String url, String from, String to) {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put(Constants.AUTH_TOKEN, "Bearer " + ehrmsApiKey);
        headerMap.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        headerMap.put("Accept", Constants.APPLICATION_JSON);

        HttpHeaders headers = new HttpHeaders();
        headerMap.forEach(headers::add);

        Map<String, String> body = new HashMap<>();
        body.put(Constants.FROM_DATE, from);
        body.put(Constants.TO_DATE, to);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
        return response.getBody();
    }

    private List<Map<String, Object>> parseCsv(String csv) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();

        if (csv == null || csv.trim().isEmpty()) {
            return rows;
        }

        try (CSVParser parser = CSVFormat.DEFAULT
                .withDelimiter('|')
                .withFirstRecordAsHeader()
                .withIgnoreSurroundingSpaces()
                .withTrim()
                .parse(new StringReader(csv))) {

            Set<String> headers = parser.getHeaderMap().keySet();

            for (CSVRecord record : parser) {
                Map<String, Object> map = new HashMap<>();
                for (String header : headers) {
                    map.put(header, record.get(header));
                }
                rows.add(map);
            }
        }
        return rows;
    }

    private Map<String, Map<String, Object>> join(List<Map<String, Object>> employees, List<Map<String, Object>> qualifications) {
        Map<String, Map<String, Object>> users = new HashMap<>();
        if (employees == null || employees.isEmpty()) {
            return users;
        }
        // Load employee master
        for (Map<String, Object> e : employees) {
            if (e == null || e.get(Constants.EMPLOYEE_ID_TITLE) == null) {
                continue;
            }
            String empId = String.valueOf(e.get(Constants.EMPLOYEE_ID_TITLE)).trim();
            if (empId.isEmpty()) {
                continue;
            }
            Map<String, Object> user = new HashMap<>();
            user.put(Constants.PROFILE, e);
            user.put(Constants.QUALIFICATIONS, new ArrayList<Map<String, Object>>());

            users.put(empId, user);
        }

        if (qualifications == null || qualifications.isEmpty()) {
            return users;
        }
        // Attach qualifications
        for (Map<String, Object> q : qualifications) {
            if (q == null || q.get(Constants.EMPLOYEE_ID_TITLE) == null) {
                continue;
            }
            String empId = String.valueOf(q.get(Constants.EMPLOYEE_ID_TITLE)).trim();
            if (empId.isEmpty()) {
                continue;
            }
            Map<String, Object> user = users.get(empId);
            if (user == null) {
                continue;
            }
            Object obj = user.get(Constants.QUALIFICATIONS);
            if (obj instanceof List) {
                ((List<Map<String, Object>>) obj).add(q);
            }
        }
        return users;
    }


    private void processUserEhrmsDataLine(Map<String, Object> user) {
        totalUsersCount.incrementAndGet();
        Map<String, String> headerValues = new HashMap<>();
        try {
            Map<String, Object> profileMap = (Map<String, Object>) user.get(Constants.PROFILE);
            if (profileMap == null) {
                profileUpdateFailedCount.incrementAndGet();
                return;
            }

            String empId = profileMap.get(Constants.EMPLOYEE_ID_TITLE) == null ? "" : profileMap.get(Constants.EMPLOYEE_ID_TITLE).toString().trim();
            String ehrmsId = profileMap.get("EmployeeCode") == null ? "" : profileMap.get("EmployeeCode").toString().trim();
            String firstName = profileMap.get("FirstName") == null ? "" : profileMap.get("FirstName").toString().trim();
            String lastName = profileMap.get("LastName") == null ? "" : profileMap.get("LastName").toString().trim();
            String empName = WordUtils.capitalizeFully((firstName + " " + lastName).trim());
            String email = profileMap.get("EmailID1") == null ? "" : profileMap.get("EmailID1").toString().trim();
            String mobile = profileMap.get("Mobile") == null ? "" : profileMap.get("Mobile").toString().trim();
            String designation = profileMap.get("designation") == null ? "" : profileMap.get("designation").toString().trim();
            String service = profileMap.get("Service") == null ? "" : profileMap.get("Service").toString().trim();
            String gender = WordUtils.capitalizeFully(profileMap.get("Gender") == null ? "" : profileMap.get("Gender").toString().trim());
            String inputDOB = profileMap.get("Dob") == null ? "" : profileMap.get("Dob").toString().trim();
            String category = profileMap.get("category") == null ? "" : profileMap.get("category").toString().trim();
            String motherTongue = WordUtils.capitalizeFully(profileMap.get("Mother Tongue") == null ? "" : profileMap.get("Mother Tongue").toString().trim());
            String state = WordUtils.capitalizeFully(profileMap.get("Home State") == null ? "" : profileMap.get("Home State").toString().trim());
            String district = WordUtils.capitalizeFully(profileMap.get("Home District") == null ? "" : profileMap.get("Home District").toString().trim());

            if (StringUtils.isEmpty(email) || StringUtils.isEmpty(mobile)) {
                profileUpdateFailedCount.incrementAndGet();
                return;
            }

            String dob = "";
            if (!StringUtils.isEmpty(inputDOB)) {
                try {
                    DateTimeFormatter inputFormatter = new DateTimeFormatterBuilder()
                            .parseLenient()
                            .appendPattern("M/d/yyyy")
                            .toFormatter();
                    dob = LocalDate.parse(inputDOB, inputFormatter)
                            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
                } catch (Exception ignored) {
                }
            }

            List<Map<String, Object>> emailSearch = searchUser(Constants.EMAIL, email);
            List<Map<String, Object>> phoneSearch = searchUser(Constants.PHONE, mobile);

            boolean hasEmail = !CollectionUtils.isEmpty(emailSearch);
            boolean hasPhone = !CollectionUtils.isEmpty(phoneSearch);

            if (hasEmail || hasPhone) {
                existingUsersCount.incrementAndGet();
            } else {
                notFoundUsersCount.incrementAndGet();
            }

            try {
                //Only phone exists
                if (!hasEmail && hasPhone) {
                    updateAndWrite(phoneSearch.get(0), user, headerValues, email, mobile,
                            designation, dob, gender, category, motherTongue, ehrmsId, true, empId, state, district);
                    profileUpdateSuccessCount.incrementAndGet();
                }

                // Both exist
                else if (hasEmail && hasPhone) {
                    String emailId = (String) emailSearch.get(0).get(Constants.USER_ID);
                    String phoneId = (String) phoneSearch.get(0).get(Constants.USER_ID);

                    if (!emailId.equalsIgnoreCase(phoneId)) {
                        log.warn("Email & Phone belong to different users for empId {}", empId);
                    }
                    updateAndWrite(emailSearch.get(0), user, headerValues, null, mobile,
                            designation, dob, gender, category, motherTongue, ehrmsId, true, empId, state, district);
                    profileUpdateSuccessCount.incrementAndGet();
                }

                // Only email exists
                else if (hasEmail) {
                    updateAndWrite(emailSearch.get(0), user, headerValues, null, mobile,
                            designation, dob, gender, category, motherTongue, ehrmsId, false, empId, state, district);
                    profileUpdateSuccessCount.incrementAndGet();
                }

            } catch (Exception ex) {
                profileUpdateFailedCount.incrementAndGet();
                log.error("Profile update failed for empId {}", empId, ex);
            }

        } catch (Exception e) {
            profileUpdateFailedCount.incrementAndGet();
            log.error("Fatal error processing user {}", user, e);
        }
    }

    private void updateAndWrite(Map<String, Object> content, Map<String, Object> user, Map<String, String> headerValues,
                                String email, String mobile, String designation, String dob,
                                String gender, String category, String motherTongue,
                                String ehrmsId, boolean skipIfMobileExists, String empId, String state, String district) throws IOException {

        Map<String, Object> request = new HashMap<>();

        String userId = content.get(Constants.USER_ID) == null ? "" : content.get(Constants.USER_ID).toString();
        String firstName = content.get(Constants.FIRSTNAME) == null ? "" : content.get(Constants.FIRSTNAME).toString();

        Map<String, Object> profile = (Map<String, Object>) content.get(Constants.PROFILE_DETAILS);
        if (MapUtils.isEmpty(profile)) {
            profile = new HashMap<>();
        }
        List<Map<String, Object>> profList = (List<Map<String, Object>>) profile.get(Constants.PROFESSIONAL_DETAILS);
        if (CollectionUtils.isEmpty(profList)) {
            profList = new ArrayList<>();
            profList.add(new HashMap<>());
        }
        Map<String, Object> prof = profList.get(0);
        if (!StringUtils.isEmpty(designation) && validateDesignationFieldValue(designation)) {
            prof.put(Constants.DESIGNATION, designation);
            profile.put(Constants.PROFILE_DESIGNATION_STATUS, Constants.VERIFIED);
            Object group = prof.get(Constants.GROUP);
            if (!StringUtils.isEmpty(group.toString())) {
                profile.put(Constants.PROFILE_GROUP_STATUS, Constants.VERIFIED);
                profile.put(Constants.PROFILE_STATUS, Constants.VERIFIED);
            }
        }

        Map<String, Object> personal = (Map<String, Object>) profile.get(Constants.PERSONAL_DETAILS);
        if (MapUtils.isEmpty(personal)) {
            personal = new HashMap<>();
            personal.put(Constants.FIRSTNAME, firstName);
        }

        // Email
        if (!StringUtils.isEmpty(email)) {
            String oldEmail = StringUtils.isEmpty(personal.get(Constants.PRIMARY_EMAIL)) ? "" : personal.get(Constants.PRIMARY_EMAIL).toString();
            if (!email.equalsIgnoreCase(oldEmail)) {
                log.info("Updating email {} → {} for empId {}", oldEmail, email, empId);
                personal.put(Constants.PRIMARY_EMAIL, email);
                request.put(Constants.EMAIL, email);
            }
        }

        // Mobile (respect skip flag)
        if (!StringUtils.isEmpty(mobile)) {
            String existingPhone = ObjectUtils.isEmpty(personal.get(Constants.MOBILE)) ? "" : String.valueOf(personal.get(Constants.MOBILE));
            if (!skipIfMobileExists && StringUtils.isEmpty(existingPhone)) {
                if (!mobile.equals(existingPhone)) {
                    log.info("Updating mobile {} → {} for empId {}", existingPhone, mobile, empId);
                    personal.put(Constants.MOBILE, mobile);
                    request.put(Constants.PHONE, mobile);
                }
            }
        }

        if (!StringUtils.isEmpty(dob)) {
            personal.put(Constants.DOB, dob);
        }
        if (!StringUtils.isEmpty(gender)) {
            personal.put(Constants.GENDER, gender);
        }
        if (!StringUtils.isEmpty(category)) {
            personal.put(Constants.CATEGORY, category);
        }
        if (!StringUtils.isEmpty(motherTongue)) {
            personal.put(Constants.DOMICILE_MEDIUM, motherTongue);
        }

        if (!StringUtils.isEmpty(ehrmsId)) {
            Map<String, Object> additional = (Map<String, Object>) profile.get(Constants.ADDITIONAL_PROPERTIES);
            if (MapUtils.isEmpty(additional)) {
                additional = new HashMap<>();
            }
            additional.put(Constants.EXTERNAL_SYSTEM, Constants.EHRMS_EXTERNAL_SYSTEM_NAME_VALUE);
            additional.put(Constants.EXTERNAL_SYSTEM_ID, ehrmsId);
            profile.put(Constants.ADDITIONAL_PROPERTIES, additional);
        }
        profile.put(Constants.PROFESSIONAL_DETAILS, profList);
        profile.put(Constants.PERSONAL_DETAILS, personal);

        request.put(Constants.USER_ID, userId);
        request.put(Constants.PROFILE_DETAILS, profile);

        Map<String, Object> response = profileUpdate(request, headerValues);
        String responseCode = MapUtils.isEmpty(response) ? "" : (String) response.get(Constants.RESPONSE_CODE);

        if (Constants.OK.equalsIgnoreCase(responseCode)) {
            updateExtendedProfile(user, userId, state, district);
            log.info("EHRMS profile updated successfully for empId {}", empId);
        } else {
            log.error("EHRMS profile update failed for empId {} → {}", empId, response);
            throw new RuntimeException("Profile update failed for userId " + userId);
        }
    }

    public Map<String, Object> profileUpdate(Map<String, Object> profileRequest, Map<String, String> headerValues) {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, profileRequest);
        String serverUrl = serverProperties.getSbUrl() + serverProperties.getLmsUserUpdatePrivatePath();
        return outboundRequestHandlerService.fetchResultUsingPatch(serverUrl, request, headerValues);
    }

    public void updateExtendedProfile(Map<String, Object> user, String userId, String state, String district) throws IOException {
        //Update Qualification
        List<Map<String, Object>> userEducationalQualifications = (List<Map<String, Object>>) user.get("qualifications");
        if (!CollectionUtils.isEmpty(userEducationalQualifications)) {
            List<Map<String, Object>> educationalQualifications = new ArrayList<>();
            for (Map<String, Object> userEducationalQualification : userEducationalQualifications) {
                Map<String, Object> objMap = new HashMap<>();
                objMap.put(Constants.DEGREE, StringUtils.isEmpty(userEducationalQualification.get("Degree")) || ((String) userEducationalQualification.get("Degree")).equalsIgnoreCase("NOT AVAILABLE") ? "Others" : (String) userEducationalQualification.get("Degree"));
                objMap.put("fieldOfStudy", StringUtils.isEmpty(userEducationalQualification.get("Field of Study")) ? "Others" : (String) userEducationalQualification.get("Field of Study"));
                objMap.put("institutionName", StringUtils.isEmpty(userEducationalQualification.get("Institute Name")) ? "Others" : (String) userEducationalQualification.get("Institute Name"));
                objMap.put("startYear", "NA");
                objMap.put("endYear", StringUtils.isEmpty(userEducationalQualification.get("Years Attended")) ? "NA" : (String) userEducationalQualification.get("Years Attended"));
                objMap.put("uuid", UUID.randomUUID().toString());
                educationalQualifications.add(objMap);
            }
            updateExtendedProfileUtils(userId, Constants.EDUCATIONAL_QUALIFICATIONS_CAMEL, educationalQualifications);
        }

        //Update location details
        if (!StringUtils.isEmpty(state) && statesAndUTs.contains(state)) {
            List<Map<String, Object>> locationDetails = new ArrayList<>();
            Map<String, Object> locationDetailsMap = new HashMap<>();
            locationDetailsMap.put(Constants.STATE, state);
            if (StringUtils.isEmpty(district) || NumberUtils.isCreatable(district)) {
                locationDetailsMap.put(Constants.DISTRICT, Constants.OTHERS);
            } else {
                locationDetailsMap.put(Constants.DISTRICT, district);
            }
            locationDetailsMap.put("uuid", UUID.randomUUID().toString());
            locationDetails.add(locationDetailsMap);
            updateExtendedProfileUtils(userId, Constants.LOCATION_DETAILS_CAMEL, locationDetails);
        }

    }

    public boolean updateExtendedProfileUtils(String userId, String contextType, List<Map<String, Object>> contextData) {
        try {
            String contextDataStr = mapper.writeValueAsString(contextData);
            Map<String, Object> query = new HashMap<>();
            query.put(Constants.USER_ID_KEY, userId);
            query.put(Constants.CONTEXT_TYPE, contextType);
            query.put(Constants.CONTEXT_DATA, contextDataStr);
            SBApiResponse insertResponse = cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_EXTENDED_PROFILE, query);
            return Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize context data for userId: {}, contextType: {}", userId, contextType);
        }
        return false;
    }

    private List<Map<String, Object>> searchUser(String key, String value) {

        HashMap<String, String> headerValues = new HashMap<>();
        headerValues.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        headerValues.put(Constants.AUTH_TOKEN, serverProperties.getSbApiKey());

        Map<String, Object> filters = new HashMap<>();
        filters.put(key, value);
        filters.put(Constants.STATUS, 1);
        Map<String, Object> searchContentRequest = new HashMap<>();
        searchContentRequest.put(Constants.FILTERS, filters);
        Map<String, Object> searchContentReqBody = new HashMap<>();
        searchContentReqBody.put(Constants.REQUEST, searchContentRequest);

        Map<String, Object> searchApiResponse = outboundRequestHandlerService.fetchResultUsingPost(
                serverProperties.getSbUrl() + serverProperties.getUserSearchEndPoint(), searchContentReqBody, headerValues);
        if (null != searchApiResponse
                && Constants.OK.equalsIgnoreCase((String) searchApiResponse.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> result = (Map<String, Object>) searchApiResponse.get(Constants.RESULT);
            Map<String, Object> resultResponse = (Map<String, Object>) result.get(Constants.RESPONSE);
            int count = (int) resultResponse.get(Constants.COUNT);
            if (count > 0) {
                return (List<Map<String, Object>>) resultResponse.get(Constants.CONTENT);
            }
        }
        return Collections.emptyList();
    }

    private Map<String, Object> updateDataBase(String jobId, Date jobStartDate, String status, int totalUser, int userFound, int userNotFound, int profileUpdateSuccessCount, int profileUpdateFailedCount) {
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.JOB_NAME, Constants.EHRMS_SYNC);
        compositeKey.put(Constants.JOB_START_DATE, jobStartDate);
        compositeKey.put(Constants.JOB_ID, jobId);

        Map<String, Object> updateAttributes = new HashMap<>();
        updateAttributes.put(Constants.STATUS, status);
        updateAttributes.put(Constants.TOTAL_USER_PROCESSED, totalUser);
        updateAttributes.put(Constants.USER_FOUND, userFound);
        updateAttributes.put(Constants.USER_NOT_FOUND, userNotFound);
        updateAttributes.put(Constants.PROFILE_UPDATE_SUCCESS_COUNT, profileUpdateSuccessCount);
        updateAttributes.put(Constants.PROFILE_UPDATE_FAILED_COUNT, profileUpdateFailedCount);
        updateAttributes.put(Constants.JOB_END_DATE, new Date());
        return cassandraOperation.updateRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.EHRMS_USER_SYNC_TABLE, updateAttributes, compositeKey);
    }

    public boolean validateDesignationFieldValue(String designation) {
        int page = 0;
        int pageSize = serverProperties.getSearchDesignationResultSize();
        String url = serverProperties.getCbPoresServiceHost() + serverProperties.getCbPoresMasterDesignationEndpoint();
        HashMap<String, String> headersValue = new HashMap<>();
        headersValue.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        Map<String, Object> searchRequest = new HashMap<>();
        searchRequest.put(Constants.PAGE_NUMBER, page);
        searchRequest.put(Constants.PAGE_SIZE, pageSize);
        searchRequest.put(Constants.REQUEST_FIELDS, new ArrayList<>());

        Map<String, Object> filterCriteria = new HashMap<>();
        filterCriteria.put(Constants.STATUS, Constants.ACTIVE_TITLE_CASE);
        filterCriteria.put(Constants.DESIGNATION, designation);
        searchRequest.put(Constants.FILTER_CRITERIA_MAP, filterCriteria);

        Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPost(url, searchRequest, headersValue);
        if (MapUtils.isEmpty(response)) {
            return false;
        }
        Map<String, Object> outerResult = (Map<String, Object>) response.get(Constants.RESULT);
        if (MapUtils.isEmpty(outerResult)) {
            return false;
        }
        Map<String, Object> innerResult = (Map<String, Object>) outerResult.get(Constants.RESULT);
        if (MapUtils.isEmpty(innerResult)) {
            return false;
        }
        List<Map<String, Object>> data = (List<Map<String, Object>>) innerResult.get(Constants.DATA);
        return !CollectionUtils.isEmpty(data);
    }
}
