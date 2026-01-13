package org.sunbird.ehrms.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
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

    @Value("{ehrms.api.user.qualifications.path}")
    private String ehrmsUserQualificationPath;

    @Value("${ehrms.api.key}")
    private String ehrmsApiKey;


    private final AtomicInteger totalUsersCount = new AtomicInteger(0);
    private final AtomicInteger existingUsersCount = new AtomicInteger(0);
    private final AtomicInteger notFoundUsersCount = new AtomicInteger(0);
    private final AtomicInteger profileUpdateSuccessCount = new AtomicInteger(0);
    private final AtomicInteger profileUpdateFailedCount = new AtomicInteger(0);

    @KafkaListener(topics = "${kafka.topic.bp.report}", groupId = "${kafka.topic.bp.report.group}")
    private void initiateEhrmsIgotDataSync(ConsumerRecord<String, String> data) {
        log.info("BPReportConsumer::processMessage.. started.");
        try {
            if (org.apache.commons.lang3.StringUtils.isNotBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    try {
                        log.info("BP report generation initiated successfully for data: {}", data.value());
                        processEhrmsIgotDataSync(data.value());
                    } catch (Exception e) {
                        log.error("Error while generating BP report for data: {}", data.value(), e);
                    }
                });
            } else {
                log.error("Error in BPReportConsumer: Invalid or empty Kafka message received");
            }
        } catch (Exception e) {
            log.error("Error while initiating BP report generation", e);
        }
    }

    public void processEhrmsIgotDataSync(String inputData) {
        log.info("BPReportConsumer:: initiateBPReportGeneration: Started");
        long duration = 0;
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> request = mapper.readValue(inputData, new TypeReference<Map<String, Object>>() {});
            String from = request.get("from_date").toString();
            String to = request.get("to_date").toString();
//        String empCsv = callEmpApi(from, to);
//        String qualCsv = callQualificationApi(from, to);

//        List<Map<String, String>> employees = parseCsv(empCsv);
//        List<Map<String, String>> quals = parseCsv(qualCsv);
            List<Map<String, Object>> employees = new ArrayList<>();

            /* ================= USER 1 ================= */
            Map<String, Object> p1 = new HashMap<>();

            p1.put("EmployeeId", "424603");
            p1.put("EmployeeCode", "UP-202");
            p1.put("FirstName", "ARVIND");
            p1.put("LastName", "SINGH");
            p1.put("MiddleName", "KUMAR");
            p1.put("EmailID1", "up202@ifs.nic.in");
            p1.put("Mobile", "9044844295");
            p1.put("designation", "");
            p1.put("category", "SC");
            p1.put("Service", "IFoS/Indian Forest Service");
            p1.put("Gender", "");
            p1.put("Dob", "1967-01-01");
            p1.put("Mother Tongue", "");
            p1.put("Joining Date", "");
            p1.put("Batch", "1991");
            p1.put("Central Deputation", "Central Staffing Scheme");
            p1.put("Home State", "UTTAR PRADESH");
            p1.put("Home District", "");
            p1.put("Cadre", "");
            employees.add(p1);


            /* ================= USER 2 ================= */
            Map<String, Object> p2 = new HashMap<>();

            p2.put("EmployeeId", "13713414");
            p2.put("EmployeeCode", "EHRM000001270");
            p2.put("FirstName", "Rkma");
            p2.put("LastName", "");
            p2.put("MiddleName", "");
            p2.put("EmailID1", "rkmalhotra61@nic.in");
            p2.put("Mobile", "9898989698");
            p2.put("designation", "Director (Geology)");
            p2.put("category", "");
            p2.put("Service", "");
            p2.put("Gender", "");
            p2.put("Dob", "1999-10-22");
            p2.put("Mother Tongue", "");
            p2.put("Joining Date", "");
            p2.put("Batch", "");
            p2.put("Central Deputation", "");
            p2.put("Home State", "");
            p2.put("Home District", "");
            p2.put("Cadre", "");

            employees.add(p2);


            /* ================= USER 3 ================= */
            Map<String, Object> p3 = new HashMap<>();

            p3.put("EmployeeId", "13713463");
            p3.put("EmployeeCode", "EHRM000001337");
            p3.put("FirstName", "DemoServices");
            p3.put("LastName", "users");
            p3.put("MiddleName", "Test");
            p3.put("EmailID1", "servicebook@gov.in");
            p3.put("Mobile", "8285066484");
            p3.put("designation", "Deputy Director General (Chemical)");
            p3.put("category", "UR");
            p3.put("Service", "AR/Assam Rifle");
            p3.put("Gender", "MALE");
            p3.put("Dob", "1990-12-12");
            p3.put("Mother Tongue", "6");
            p3.put("Joining Date", "2010-05-05");
            p3.put("Batch", "");
            p3.put("Central Deputation", "No");
            p3.put("Home State", "UTTARAKHAND");
            p3.put("Home District", "BAGESHWAR");
            p3.put("Cadre", "");
            employees.add(p3);

            Map<String, String> qualification = new HashMap<>();
            List<Map<String, String>> quals = new ArrayList<>();

            qualification.put("EmployeeId", "0");
            qualification.put("Degree", "BBA (Bachelor of Business Administration)");
            qualification.put("Field of Study", "");
            qualification.put("Institute Name", "");
            qualification.put("Years Attended", "2004");
            qualification.put("Organization", "UPTU Lucknow");
            quals.add(qualification);


            Map<String, Map<String, Object>> merged = join(employees, quals);

            // Example usage (send to iGOT or log)
            for (String empCode : merged.keySet()) {
                processUserEhrmsDataLine(merged.get(empCode));
            }
        } catch (Exception e) {
            log.error(String.format("Error in the scheduler to generate the BP report %s", e.getMessage()),
                    e);
        }
        duration = System.currentTimeMillis() - startTime;
        log.info("BPReportConsumer:: initiateBPReportGeneration: Completed. Time taken: "
                + duration + " milli-seconds");
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
        headerMap.put("Authorization", "Bearer " + ehrmsApiKey);
        headerMap.put("Content-Type", "application/json");
        headerMap.put("Accept", "application/json");

        HttpHeaders headers = new HttpHeaders();
        headerMap.forEach(headers::add);

        Map<String, String> body = new HashMap<>();
        body.put("from_date", from);
        body.put("to_date", to);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
        return response.getBody();
    }

    private List<Map<String, String>> parseCsv(String csv) throws Exception {
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new StringReader(csv));

        List<Map<String, String>> rows = new ArrayList<>();
        for (CSVRecord record : parser) {
            Map<String, String> map = new HashMap<>();
            for (String h : parser.getHeaderMap().keySet()) {
                map.put(h, record.get(h));
            }
            rows.add(map);
        }
        return rows;
    }

    private Map<String, Map<String, Object>> join(
            List<Map<String, Object>> employees,
            List<Map<String, String>> qualifications) {

        Map<String, Map<String, Object>> users = new HashMap<>();

        // Load employee master
        for (Map<String, Object> e : employees) {
            String empId = e.get("EmployeeId").toString();

            Map<String, Object> user = new HashMap<>();
            user.put("profile", e);
            user.put("qualifications", new ArrayList<Map<String, String>>());

            users.put(empId, user);
        }

        // Attach qualifications
        for (Map<String, String> q : qualifications) {
            String empId = q.get("EmployeeId").toString();

            if (users.containsKey(empId)) {
                List<Map<String, String>> list =
                        (List<Map<String, String>>) users.get(empId).get("qualifications");
                list.add(q);
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

            String empId = profileMap.get("EmployeeId") == null ? "" : profileMap.get("EmployeeId").toString().trim();
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
                            designation, dob, gender, category, motherTongue, ehrmsId, false, empId, state, district);
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
                            designation, dob, gender, category, motherTongue, ehrmsId, false, empId, state, district);
                    profileUpdateSuccessCount.incrementAndGet();
                }

                // Only email exists
                else if (hasEmail) {
                    updateAndWrite(emailSearch.get(0), user, headerValues, null, mobile,
                            designation, dob, gender, category, motherTongue, ehrmsId, true, empId, state, district);
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
        if (profile == null) {
            profile = new HashMap<>();
        }

        List<Map<String, Object>> profList = (List<Map<String, Object>>) profile.get(Constants.PROFESSIONAL_DETAILS);
        if (CollectionUtils.isEmpty(profList)) {
            profList = new ArrayList<>();
            profList.add(new HashMap<>());
        }

        Map<String, Object> prof = profList.get(0);

        if (!StringUtils.isEmpty(designation)) {
            prof.put(Constants.DESIGNATION, designation);
            profile.put(Constants.PROFILE_DESIGNATION_STATUS, Constants.VERIFIED);

            Object group = prof.get(Constants.GROUP);
            if (group != null && !StringUtils.isEmpty(group.toString())) {
                profile.put(Constants.PROFILE_GROUP_STATUS, Constants.VERIFIED);
                profile.put(Constants.PROFILE_STATUS, Constants.VERIFIED);
            }
        }

        Map<String, Object> personal = (Map<String, Object>) profile.get(Constants.PERSONAL_DETAILS);
        if (personal == null) {
            personal = new HashMap<>();
            personal.put(Constants.FIRSTNAME, firstName);
        }

        // Email
        if (!StringUtils.isEmpty(email)) {
            String oldEmail = personal.get(Constants.PRIMARY_EMAIL) == null ? "" : personal.get(Constants.PRIMARY_EMAIL).toString();
            if (!email.equalsIgnoreCase(oldEmail)) {
                log.info("Updating email {} → {} for empId {}", oldEmail, email, empId);
                personal.put(Constants.PRIMARY_EMAIL, email);
                request.put(Constants.EMAIL, email);
            }
        }

        // Mobile (respect skip flag)
        if (!StringUtils.isEmpty(mobile)) {
            String existingPhone = content.get(Constants.PHONE) == null ? "" : content.get(Constants.PHONE).toString();

            if (!skipIfMobileExists || StringUtils.isEmpty(existingPhone)) {
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
            if (additional == null) {
                additional = new HashMap<>();
            }
            additional.put(Constants.EXTERNAL_SYSTEM, "DoPT eHRMS");
            additional.put(Constants.EXTERNAL_SYSTEM_ID, ehrmsId);
            profile.put(Constants.ADDITIONAL_PROPERTIES, additional);
        }
        profile.put(Constants.PROFESSIONAL_DETAILS, profList);
        profile.put(Constants.PERSONAL_DETAILS, personal);

        request.put(Constants.USER_ID, userId);
        request.put(Constants.PROFILE_DETAILS, profile);

        Map<String, Object> response = profileUpdate(request, headerValues);
        String responseCode = response == null ? "" : (String) response.get(Constants.RESPONSE_CODE);

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
                objMap.put("degree", StringUtils.isEmpty(userEducationalQualification.get("degree")) || ((String) userEducationalQualification.get("degree")).equalsIgnoreCase("NOT AVAILABLE") ? "Others" : (String) userEducationalQualification.get("degree"));
                objMap.put("fieldOfStudy", StringUtils.isEmpty(userEducationalQualification.get("MajorSubject")) ? "Others" : (String) userEducationalQualification.get("MajorSubject"));
                objMap.put("institutionName", StringUtils.isEmpty(userEducationalQualification.get("Institution")) ? "Others" : (String) userEducationalQualification.get("Institution"));
                objMap.put("startYear", "NA");
                objMap.put("endYear", StringUtils.isEmpty(userEducationalQualification.get("passing_year")) ? "NA" : (String) userEducationalQualification.get("passing_year"));
                objMap.put("uuid", UUID.randomUUID().toString());
                educationalQualifications.add(objMap);
            }
            updateExtendedProfileUtils(userId, "educationalQualifications", educationalQualifications);
        }

        //Update location details
        List<String> statesAndUTs = Arrays.asList("Andaman And Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh", "Chhattisgarh", "Delhi", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu And Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "The Dadra And Nagar Haveli And Daman And Diu", "Tripura", "Uttarakhand", "Uttar Pradesh", "West Bengal");
        if (!StringUtils.isEmpty(state) && statesAndUTs.contains(state)) {
            List<Map<String, Object>> locationDetails = new ArrayList<>();
            Map<String, Object> locationDetailsMap = new HashMap<>();
            locationDetailsMap.put(Constants.STATE, state);
            if (StringUtils.isEmpty(district) || NumberUtils.isCreatable(district)) {
                locationDetailsMap.put("district", "Others");
            } else {
                locationDetailsMap.put("district", district);
            }
            locationDetailsMap.put("uuid", UUID.randomUUID().toString());
            locationDetails.add(locationDetailsMap);
            updateExtendedProfileUtils(userId, "locationDetails", locationDetails);

        }

    }

    public boolean updateExtendedProfileUtils(String userId, String contextType, List<Map<String, Object>> contextData) {
        try {
            String contextDataStr = mapper.writeValueAsString(contextData);
            Map<String, Object> query = new HashMap<>();
            query.put(Constants.USER_ID_KEY, userId);
            query.put(Constants.CONTEXT_TYPE, contextType);
            query.put("contextData", contextDataStr);
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
        headerValues.put("Authorization", serverProperties.getSbApiKey());

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
        return null;

    }
}
