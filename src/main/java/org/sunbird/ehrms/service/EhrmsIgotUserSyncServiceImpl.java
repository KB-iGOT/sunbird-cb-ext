package org.sunbird.ehrms.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
public class EhrmsIgotUserSyncServiceImpl {

    private final Logger log = LoggerFactory.getLogger(getClass().getName());

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    CassandraOperation cassandraOperation;


    private static final String TOKEN = "YOUR_BEARER_TOKEN";

    public boolean userEhrmsDataUpdate() throws Exception {
        String from = "01-07-2025";
        String to = "31-12-2025";

        String empCsv = callEmpApi(from, to);
        String qualCsv = callQualificationApi(from, to);

        List<Map<String, String>> employees = parseCsv(empCsv);
        List<Map<String, String>> quals = parseCsv(qualCsv);

        Map<String, Map<String, Object>> merged = join(employees, quals);

        // Example usage (send to iGOT or log)
        for (String empCode : merged.keySet()) {
            System.out.println(empCode + " => " + merged.get(empCode));
        }
    }

    // ================== API Calls ==================

    private String callEmpApi(String from, String to) throws Exception {
        return callApi(
                "https://api-ehrms.prod.karmayogibharat.net/clientapi/api/clientapi/v1/profile/emp-data",
                from, to
        );
    }

    private String callQualificationApi(String from, String to) throws Exception {
        return callApi(
                "https://api-ehrms.prod.karmayogibharat.net/clientapi/api/clientapi/v1/profile/qualification-data",
                from, to
        );
    }

    private String callApi(String url, String from, String to) throws Exception {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost post = new HttpPost(url);

        post.setHeader("Authorization", "Bearer " + TOKEN);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");

        String body = "{\"from_date\":\"" + from + "\",\"to_date\":\"" + to + "\"}";
        post.setEntity(new StringEntity(body));

        HttpResponse response = client.execute(post);
        return EntityUtils.toString(response.getEntity());
    }

    // ================== CSV Parsing ==================

    private List<Map<String, String>> parseCsv(String csv) throws Exception {
        CSVParser parser = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new StringReader(csv));

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

    // ================== JOIN on employeeCode ==================

    private Map<String, Map<String, Object>> join(
            List<Map<String, String>> employees,
            List<Map<String, String>> qualifications) {

        Map<String, Map<String, Object>> users = new HashMap<>();

        // Load employee master
        for (Map<String, String> e : employees) {
            String empCode = e.get("employeeCode");

            Map<String, Object> user = new HashMap<>();
            user.put("profile", e);
            user.put("qualifications", new ArrayList<Map<String, String>>());

            users.put(empCode, user);
        }

        // Attach qualifications
        for (Map<String, String> q : qualifications) {
            String empCode = q.get("employeeCode");

            if (users.containsKey(empCode)) {
                List<Map<String, String>> list =
                        (List<Map<String, String>>) users.get(empCode).get("qualifications");
                list.add(q);
            }
        }

        return users;
    }

    private void processUserEhrmsDataLine(String line, Map<String, String> headerValues) throws IOException {
        String[] user = line.split("\\:");
        if (user.length < 11) {
            writeListIntoExistingTextFile(line, "InvalidDataFormat");
            return;
        }

        String empId = user[0].trim();
        String ehrmsId = user[1].trim();
        String empName = WordUtils.capitalizeFully(StringUtils.isEmpty(user[2].trim()) ? "" : user[2].trim());
        String email = user[3].trim().toLowerCase();
        String mobile = user[4].trim();
        if (StringUtils.isEmpty(email) || StringUtils.isEmpty(mobile)) {
            writeListIntoExistingTextFile(line, "InvalidDataFormat");
            return;
        }
        String designation = user[5].trim();
        String category = user[6].trim();
        String gender = WordUtils.capitalizeFully(StringUtils.isEmpty(user[7].trim()) ? "" : user[7].trim());
        String inputDOB = user[8].trim();
        String motherTongue = WordUtils.capitalizeFully(StringUtils.isEmpty(user[9].trim()) ? "" : user[9].trim());
        String state = WordUtils.capitalizeFully(StringUtils.isEmpty(user[10].trim()) ? "" : user[10].trim());
        // String district = WordUtils.capitalizeFully(StringUtils.isEmpty(user[11].trim()) ? "" : user[11].trim());
        String district = "";

        String dob = "";
        if (!StringUtils.isEmpty(inputDOB)) {
            try {
                DateTimeFormatter inputFormatter = new DateTimeFormatterBuilder()
                        .parseLenient()
                        .appendPattern("M/d/yyyy")
                        .toFormatter();
                DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
                LocalDate date = LocalDate.parse(inputDOB, inputFormatter);
                dob = date.format(outputFormatter);
            } catch (DateTimeParseException e) {
                dob = "";
            }
        }

        try {
            List<Map<String, Object>> emailSearch = searchUser(Constants.EMAIL, email);
            List<Map<String, Object>> phoneSearch = searchUser(Constants.PHONE, mobile);

            boolean hasEmail = !CollectionUtils.isEmpty(emailSearch);
            boolean hasPhone = !CollectionUtils.isEmpty(phoneSearch);

            // 1️⃣ Account exists only with phone → add missing email
            if (!hasEmail && hasPhone) {
                writeListIntoExistingTextFile(line + ":" + phoneSearch.get(0).get(Constants.USER_ID), "EhrmsUserHasAccountsWithProvidedPhoneOnly");
                updateAndWrite(phoneSearch.get(0), line, headerValues, email, mobile,
                        designation, dob, gender, category, motherTongue, ehrmsId, false, empId, state, district);
            }
            // 2️⃣ Both exist and belong to same user
            else if (hasEmail && hasPhone) {
                String idEmail = (String) emailSearch.get(0).get(Constants.USER_ID);
                String idPhone = (String) phoneSearch.get(0).get(Constants.USER_ID);
                if (idEmail.equalsIgnoreCase(idPhone)) {
                    writeListIntoExistingTextFile(line + ":" + emailSearch.get(0).get(Constants.USER_ID), "EhrmsUserHasSameAccounts");
                    updateAndWrite(emailSearch.get(0), line, headerValues, null, mobile,
                            designation, dob, gender, category, motherTongue, ehrmsId, false, empId, state, district);
                } else {
                    log.info("User has two different account: {}", line);
                    writeListIntoExistingTextFile(line, "EhrmsUserHasTwoDifferentAccounts");
                }
            }
            // 3️⃣ Account exists only with email → add mobile only if missing
            else if (hasEmail) {
                writeListIntoExistingTextFile(line + ":" + emailSearch.get(0).get(Constants.USER_ID), "EhrmsUserHasAccountsWithProvidedEmailOnly");
                updateAndWrite(emailSearch.get(0), line, headerValues, null, mobile,
                        designation, dob, gender, category, motherTongue, ehrmsId, true, empId, state, district);
            } else {
                writeListIntoExistingTextFile(line, "EhrmsDataUserNotFound");
            }

        } catch (Exception e) {
            writeListIntoExistingTextFile(line, "EhrmsDataUpdateException");
        }
    }

    private void updateAndWrite(Map<String, Object> content, String line, Map<String, String> headerValues,
                                String email, String mobile, String designation, String dob,
                                String gender, String category, String motherTongue,
                                String ehrmsId, boolean skipIfMobileExists, String empId, String state, String district) throws IOException {
        Map<String, Object> request = new HashMap<>();

        String userId = (String) content.get(Constants.USER_ID);
        String firstName = (String) content.get(Constants.FIRSTNAME);
        Map<String, Object> profile = (Map<String, Object>) content.get(Constants.PROFILE_DETAILS);
        if (CollectionUtils.isEmpty(profile)) {
            profile = new HashMap<>();
        }

        // --- Professional details ---
        List<Map<String, Object>> profList = (List<Map<String, Object>>) profile.get(Constants.PROFESSIONAL_DETAILS);
        if (CollectionUtils.isEmpty(profList)) {
            profList = new ArrayList<>();
            profList.add(new HashMap<String, Object>());
        }
        Map<String, Object> prof = profList.get(0);
        if (!StringUtils.isEmpty(designation)) {
            prof.put(Constants.DESIGNATION, designation);
            profile.put("profileDesignationStatus", "VERIFIED");

            if (prof.containsKey(Constants.GROUP) && !StringUtils.isEmpty((String) prof.get(Constants.GROUP))) {
                profile.put("profileGroupStatus", "VERIFIED");
                profile.put("profileStatus", "VERIFIED");
            }
        }

        // --- Personal details ---
        Map<String, Object> personal = (Map<String, Object>) profile.get(Constants.PERSONAL_DETAILS);
        if (MapUtils.isEmpty(personal)) {
            personal = new HashMap<>();
            personal.put("firstname", firstName);
        }

        if (!StringUtils.isEmpty(email)) {
            log.info("Updating Email for user: {}", line);
            String previousEmail = personal.getOrDefault(Constants.PRIMARY_EMAIL, "").toString();
            personal.put(Constants.PRIMARY_EMAIL, email);
            request.put(Constants.EMAIL, email);
            writeListIntoExistingTextFile(line + ":" + userId + ":" + previousEmail, "EhrmsUpdatedUserEmail");
        }

        // ✅ Only update mobile if missing or empty (skip if already exists and flag is true)
        if (!StringUtils.isEmpty(mobile)) {
            Object existingPhone = (String) content.get(Constants.PHONE);
            if (skipIfMobileExists && StringUtils.isEmpty(existingPhone)) {
                log.info("Updating mobile for user: {}", line);
                personal.put(Constants.MOBILE, mobile);
                request.put(Constants.PHONE, mobile);
                writeListIntoExistingTextFile(line + ":" + userId, "EhrmsUpdatedUserMobile");

            }
        }

        if (!StringUtils.isEmpty(dob)) {
            personal.put("dob", dob);
        }
        if (!StringUtils.isEmpty(gender)) {
            personal.put("gender", gender);
        }
        if (!StringUtils.isEmpty(category)) {
            personal.put("category", category);
        }
        if (!StringUtils.isEmpty(motherTongue)) {
            personal.put("domicileMedium", motherTongue);
        }


        // --- Additional details ---
        if (!StringUtils.isEmpty(ehrmsId)) {
            Map<String, Object> additional = (Map<String, Object>) profile.get(Constants.ADDITIONAL_PROPERTIES);
            if (MapUtils.isEmpty(additional)) {
                additional = new HashMap<>();
            }
            additional.put(Constants.EXTERNAL_SYSTEM, "DoPT eHRMS");
            additional.put(Constants.EXTERNAL_SYSTEM_ID, ehrmsId);
            profile.put(Constants.ADDITIONAL_PROPERTIES, additional);
        } else {
            writeListIntoExistingTextFile(line + ":" + userId, "EhrmsIdIsNull");
        }

        // --- Final profile structure ---
        profile.put(Constants.PROFESSIONAL_DETAILS, profList);
        profile.put(Constants.PERSONAL_DETAILS, personal);

        // --- Prepare update request ---
        request.put(Constants.USER_ID, content.get(Constants.USER_ID));
        request.put(Constants.PROFILE_DETAILS, profile);

        // --- Call update API ---
        Map<String, Object> response = profileUpdate(request, headerValues);
        String output = Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))
                ? "EhrmsDataUpdateSuccess"
                : "EhrmsDataUpdateFailed";
        if (output.equalsIgnoreCase("EhrmsDataUpdateSuccess")) {
            updateExtendedProfile(line, empId, userId, state, district);
        }

        writeListIntoExistingTextFile(line + ":" + userId, output);
    }


    public Map<String, Object> profileUpdate(Map<String, Object> profileRequest, Map<String, String> headerValues) {

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, profileRequest);
        String serverUrl = serverProperties.getSbUrl() + serverProperties.getLmsUserUpdatePrivatePath();
        Map<String, Object> responseData = outboundRequestHandlerService.fetchResultUsingPatch(serverUrl, request, headerValues);

        return responseData;

    }

    public void updateExtendedProfile(String line, String empId, String userId, String state, String district) throws IOException {
        boolean educationalQualificationUpdatedFlag = false;
        boolean locationDetailsUpdatedFlag = false;
        //Update Qualification
        List<Map<String, Object>> userEducationalQualifications = getQualifications(empId);
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
            educationalQualificationUpdatedFlag = updateExtendedProfileUtils(userId, "educationalQualifications", educationalQualifications);
        }

        //Update location details
        List<String> statesAndUTs = Arrays.asList("Andaman And Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chandigarh", "Chhattisgarh", "Delhi", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu And Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "The Dadra And Nagar Haveli And Daman And Diu", "Tripura", "Uttarakhand", "Uttar Pradesh", "West Bengal");
        if (!StringUtils.isEmpty(state) && statesAndUTs.contains(state)) {
            List<Map<String, Object>> locationDetails = new ArrayList<>();
            Map<String, Object> locationDetailsMap = new HashMap<>();
            locationDetailsMap.put("state", state);
            if (StringUtils.isEmpty(district) || NumberUtils.isCreatable(district)) {
                locationDetailsMap.put("district", "Others");
            } else {
                locationDetailsMap.put("district", district);
            }
            locationDetailsMap.put("uuid", UUID.randomUUID().toString());
            locationDetails.add(locationDetailsMap);
            locationDetailsUpdatedFlag = updateExtendedProfileUtils(userId, "locationDetails", locationDetails);

        }
        if (educationalQualificationUpdatedFlag || locationDetailsUpdatedFlag) {
            writeListIntoExistingTextFile(line + ":" + userId, "EhrmsUpdatedExtendedProfile");
        }

    }

    public List<Map<String, Object>> getQualifications(String empId) {
        return repository.getUserQualifications(empId);
    }

    public boolean updateExtendedProfileUtils(String userId, String contextType, List<Map<String, Object>> contextData) {
        try {
            String contextDataStr = mapper.writeValueAsString(contextData);
            Map<String, Object> query = new HashMap<>();
            query.put("userid", userId);
            query.put("contexttype", contextType);
            query.put("contextdata", contextDataStr);
            SBApiResponse insertResponse = cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_EXTENDED_PROFILE, query);
            return Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize context data for userId: {}, contextType: {}", userId, contextType);
        }
        return false;
    }

    private synchronized void writeListIntoExistingTextFile(String value, String fileN) throws IOException {
        String fileName = fileN + "-" + LocalDate.now() + ".csv";
        String filePath = (fileName);
        Path path = Paths.get(filePath);
        boolean fileExists = Files.exists(path);
        BufferedWriter bf = new BufferedWriter(new FileWriter(filePath, true));
        try {
            if (!fileExists) {
                new File(filePath);
            }
            bf.write(value);
            bf.newLine();
            bf.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bf.close();
            } catch (Exception e) {
            }
        }
    }

    private List<Map<String, Object>> searchUser(String key, String value) {

        HashMap<String, String> headerValues = new HashMap<>();
        headerValues.put(Constants.CONTENT_TYPE, Constants.CONTENT_TYPE_JSON);
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
