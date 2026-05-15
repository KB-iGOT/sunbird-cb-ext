package org.sunbird.org.service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.common.model.SBApiOrgSearchRequest;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.consumer.KafkaProducer;
import org.sunbird.org.model.OrgHierarchy;
import org.sunbird.org.model.OrgHierarchyInfo;
import org.sunbird.org.repository.OrgHierarchyRepository;
import org.sunbird.common.util.IndexerService;

@Service
public class ExtendedOrgServiceImpl implements ExtendedOrgService {
	private Logger logger = LoggerFactory.getLogger(getClass().getName());

	@Autowired
	OutboundRequestHandlerServiceImpl outboundService;

	@Autowired
	CbExtServerProperties configProperties;

	@Autowired
	OrgHierarchyRepository orgRepository;

    @Autowired
    KafkaProducer kafkaProducer;


	@Autowired
	IndexerService indexerService;

	@Autowired
	CbExtServerProperties serverConfig;
	@Value("${org.search.list.batch.size}")
	private int batchsize;

	@Value("${user.search.limit}")
	private int userSearchLimit;

    @Value("${kafka.topics.org.hierarchy.framework.new.org.event}")
    private String kafkaTopicCreateHierarchyFramework;

    ObjectMapper objectMapper = new ObjectMapper();


    @SuppressWarnings("unchecked")
	@Override
	public SBApiResponse createOrg(Map<String, Object> request, String userToken) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_EXT_CREATE);
		try {
			String errMsg = validateOrgRequest(request);
			if (!StringUtils.isEmpty(errMsg)) {
				response.getParams().setErrmsg(errMsg);
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				return response;
			}

			Map<String, Object> requestData = (Map<String, Object>) request.get(Constants.REQUEST);
			String orgId = checkOrgExist((String) requestData.get(Constants.CHANNEL), userToken);
			String orgType = (String) requestData.get(Constants.ORGANIZATION_TYPE);
			String channelName = null;
			boolean dbUpdateRequired = false;
			boolean orgCreatedWithNewChannel = false;
            boolean isNewOrgCreated = false;

			if (StringUtils.isEmpty(orgId)) {
				// There is no org exist for given Channel. We can simply create the same in
				// system.
				errMsg = validateRequestFieldsOrganisationCreate(request,response);
				if (!StringUtils.isEmpty(errMsg)) return response;
				fetchStateOrMinistryDetails(request);
				orgId = createOrgInSunbird(request, (String) requestData.get(Constants.CHANNEL), userToken);
				if (StringUtils.isBlank(orgId)) {
					response.getParams().setErrmsg("Failed to create organisation in Sunbird.");
					response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
					return response;
				}
				dbUpdateRequired = true;
                isNewOrgCreated = true;
			} else {
				// The channel already exist. We need to check OrgHierarchy table for duplicate.
				if (Constants.STATE.equalsIgnoreCase(orgType)
						|| Constants.MINISTRY.equalsIgnoreCase(orgType)) {
					// We are not allowing duplicates @ L1 -- Need to throw error
					response.getParams().setErrmsg("Organisation is already exist.");
					response.setResponseCode(HttpStatus.BAD_REQUEST);
					return response;
				} else {
					OrgHierarchy existingDBRecord = orgRepository.findByOrgNameAndParentMapId(
							(String) requestData.get(Constants.CHANNEL),
							(String) requestData.get(Constants.PARENT_MAP_ID));
					if (existingDBRecord == null) {
						channelName = prepareChannelName((String) requestData.get(Constants.PARENT_MAP_ID),
								requestData);
						channelName = channelName + (String) requestData.get(Constants.CHANNEL);
						requestData.put(Constants.CHANNEL, channelName);
						orgId = createOrgInSunbird(request, (String) requestData.get(Constants.CHANNEL), userToken);
						if (StringUtils.isBlank(orgId)) {
							response.getParams().setErrmsg("Failed to create organisation in Sunbird.");
							response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
							return response;
						}
						dbUpdateRequired = true;
						orgCreatedWithNewChannel = true;
					} else if (StringUtils.isBlank(existingDBRecord.getSbOrgId())) {
						existingDBRecord.setSbOrgId(orgId);
						if (StringUtils.isEmpty(existingDBRecord.getSbRootOrgId())) {
							existingDBRecord
									.setSbRootOrgId(fetchRootOrgId((String) requestData.get(Constants.PARENT_MAP_ID)));
						}
						orgRepository.save(existingDBRecord);
					} else if (existingDBRecord.getSbOrgId().equalsIgnoreCase(orgId)) {
						response.getParams().setErrmsg("Duplicate Record Found in OrgHierarchy. Contact Admin");
						response.setResponseCode(HttpStatus.BAD_REQUEST);
						return response;
					}
				}
			}

			if (dbUpdateRequired) {
				OrgHierarchy existingDBRecord = orgRepository.findByOrgNameAndParentMapId(
						(String) requestData.get(Constants.CHANNEL), (String) requestData.get(Constants.PARENT_MAP_ID));
				if (existingDBRecord != null) {
					existingDBRecord.setSbOrgId(orgId);
					if (StringUtils.isEmpty(existingDBRecord.getSbRootOrgId())) {
						existingDBRecord
								.setSbRootOrgId(fetchRootOrgId((String) requestData.get(Constants.PARENT_MAP_ID)));
					}
					orgRepository.save(existingDBRecord);
				} else {
					// We just created with given channel name. but this is new record in
					// orgHierarchy...
					// By calling prepareChannelName we will update L1 and L2 details.
					prepareChannelName((String) requestData.get(Constants.PARENT_MAP_ID), requestData);
					channelName = (String) requestData.get(Constants.CHANNEL);
					orgCreatedWithNewChannel = true;
				}
			}

			if (orgCreatedWithNewChannel) {
				Map<String, Object> updateRequest = new HashMap<String, Object>();
				String orgName = (String) requestData.get(Constants.ORG_NAME);
				updateRequest.put(Constants.CHANNEL, (String) requestData.get(Constants.CHANNEL));
				updateRequest.put(Constants.SB_ORG_ID, orgId);
				updateRequest.put(Constants.ORG_NAME, orgName);
				updateRequest.put(Constants.SB_ORG_TYPE, orgType);
				updateRequest.put(Constants.L1_MAP_ID, (String) requestData.get(Constants.L1_MAP_ID));
				updateRequest.put(Constants.L2_MAP_ID, (String) requestData.get(Constants.L2_MAP_ID));
				updateRequest.put(Constants.L1_ORG_NAME, (String) requestData.get(Constants.L1_ORG_NAME));
				updateRequest.put(Constants.L2_ORG_NAME, (String) requestData.get(Constants.L2_ORG_NAME));

				String mapId = (String) requestData.get(Constants.MAP_ID);
				if (StringUtils.isEmpty(mapId)) {
					// There is a possibility that this Org already exists in table. Get the MapId
					// if so.
					fetchMapIdFromDB(requestData);
					mapId = (String) requestData.get(Constants.MAP_ID);
				}
				String orgCode = (String) requestData.get(Constants.ORG_CODE);
				String sbRootOrgid = (String) requestData.get(Constants.SB_ROOT_ORG_ID);
				updateRequest.put(Constants.SB_SUB_ORG_TYPE, requestData.get(Constants.ORGANIZATION_SUB_TYPE));
				if (!StringUtils.isEmpty(mapId)) {
					updateRequest.put(Constants.MAP_ID, mapId);
				} else {
					mapId = createMapId(requestData);
					updateRequest.put(Constants.MAP_ID, mapId);
					updateRequest.put(Constants.ORG_CODE, mapId);
				}
				if (!StringUtils.isEmpty(orgCode)) {
					updateRequest.put(Constants.ORG_CODE, orgCode);
				}
				if (!Constants.STATE.equalsIgnoreCase(orgType) && !Constants.MINISTRY.equalsIgnoreCase(orgType)) {
					updateRequest.put(Constants.PARENT_MAP_ID, requestData.get(Constants.PARENT_MAP_ID));
					if (!StringUtils.isEmpty(sbRootOrgid)) {
						updateRequest.put(Constants.SB_ROOT_ORG_ID, sbRootOrgid);
					} else {
						updateRequest.put(Constants.SB_ROOT_ORG_ID,
								fetchRootOrgId((String) requestData.get(Constants.PARENT_MAP_ID)));
					}
				} else {
					updateRequest.put(Constants.PARENT_MAP_ID, Constants.SPV);
				}
				if (!StringUtils.isEmpty((String) requestData.get(Constants.MAP_ID))) {
					ObjectMapper om = new ObjectMapper();
					logger.info("Need to update the record here... " + om.writeValueAsString(updateRequest));
					if (ObjectUtils.isEmpty(updateRequest.get(Constants.SB_ROOT_ORG_ID))) {
						orgRepository.updateOrgIdForChannel(channelName,
								(String) updateRequest.get(Constants.SB_ORG_ID));
					} else {
						orgRepository.updateSbOrgIdAndSbOrgRootIdForChannel(channelName,
								(String) updateRequest.get(Constants.SB_ORG_ID),
								(String) updateRequest.get(Constants.SB_ROOT_ORG_ID));
					}
				} else {
					OrgHierarchy newOrg = new OrgHierarchy(orgName, channelName, mapId,
							(String) updateRequest.get(Constants.PARENT_MAP_ID));
					orgRepository.save(getOrgRecord(updateRequest, newOrg));
				}
				response.getResult().put(Constants.ORGANIZATION_ID, orgId);
				response.getResult().put(Constants.RESPONSE, Constants.SUCCESS);
			}
            //Push Message to Kafka
            if(isNewOrgCreated || orgCreatedWithNewChannel){
                requestData.put(Constants.ORG_ID, orgId);
                kafkaProducer.push(kafkaTopicCreateHierarchyFramework, requestData);
            }
		} catch (Exception e) {
			logger.error("Failed to create user. Exception: ", e);
			response.getParams().setErrmsg(e.getMessage());
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return response;
	}

	@Override
	public SBApiResponse listOrg(String parentMapId) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_LIST);
		if (StringUtils.isEmpty(parentMapId)) {
			parentMapId = Constants.SPV;
		}

		List<OrgHierarchy> orgHierarchyList = null;
		if (Constants.MINISTRY.equalsIgnoreCase(parentMapId)
				|| Constants.STATE.equalsIgnoreCase(parentMapId)) {
			orgHierarchyList = orgRepository.findAllBySbOrgType(parentMapId);
		} else {
			orgHierarchyList = orgRepository.findAllByParentMapId(parentMapId);
		}
		if (CollectionUtils.isNotEmpty(orgHierarchyList)) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			responseMap.put(Constants.CONTENT, orgHierarchyList);
			responseMap.put(Constants.COUNT, orgHierarchyList.size());
			response.put(Constants.RESPONSE, responseMap);
		} else {
			Map<String, Object> responseMap = new HashMap<>();
			responseMap.put(Constants.CONTENT, orgHierarchyList);
			responseMap.put(Constants.COUNT, orgHierarchyList.size());
			response.put(Constants.RESPONSE, responseMap);
			response.getParams().setErrmsg("No child org found for Id: " + parentMapId);
		}

		return response;
	}

	@Override
	public SBApiResponse orgExtSearch(Map<String, Object> request) throws Exception {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_EXT_SEARCH);
		try {
			String errMsg = validateOrgSearchReq(request);
			if (!StringUtils.isEmpty(errMsg)) {
				response.getParams().setErrmsg(errMsg);
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				return response;
			}

			Map<String, Object> requestData = (Map<String, Object>) request.get(Constants.REQUEST);
			Map<String, Object> filters = (Map<String, Object>) requestData.get(Constants.FILTERS);
			// sbRootOrgId is State Id. Let's get all the children.
			String sbRootOrgId = (String) filters.get(Constants.SB_ROOT_ORG_ID);
			String query = Optional.ofNullable(requestData.get(Constants.QUERY)).map(Object::toString).orElse(null);

			int limit = Optional.ofNullable((Integer) requestData.get(Constants.LIMIT))
					.filter(l -> l > 0) // Ensure positive value
					.orElse(20); // Default to 20 if null or <= 0
			int offset = Optional.ofNullable((Integer) requestData.get(Constants.OFFSET))
					.filter(o -> o >= 0) // Ensure non-negative value
					.orElse(0);          // Default to 0 if null or negative

			int page = offset / limit;
			Pageable pageable = PageRequest.of(page, limit);

			Page<String> orgIdPage;
			if (StringUtils.isNotEmpty(query)) {
				orgIdPage = orgRepository.findAllBySbRootOrgIdAndQuery(sbRootOrgId, query, pageable);
			} else {
				orgIdPage = orgRepository.findAllBySbRootOrgId(sbRootOrgId, pageable);
			}
			long totalOrgCount = orgIdPage.getTotalElements();
			List<String> orgIdList = orgIdPage.getContent();
			if (CollectionUtils.isNotEmpty(orgIdList)) {
				SBApiOrgSearchRequest orgSearchRequest = new SBApiOrgSearchRequest();
				orgSearchRequest.getFilters().setId(orgIdList);
				if (!ProjectUtil.isStringNullOREmpty((String) requestData.get(Constants.QUERY))) {
					orgSearchRequest.setQuery((String) requestData.get(Constants.QUERY));
				}
				orgSearchRequest.setSortBy((Map<String, String>) requestData.get(Constants.SORT_BY_KEYWORD));
				logger.info("Constructing the request body for organization search with the necessary parameters.");
				Map<String, Object> orgSearchRequestBody = new HashMap<String, Object>() {
					private static final long serialVersionUID = 1L;
					{
						put(Constants.REQUEST, orgSearchRequest);
					}
				};
				Map<String, String> headers = new HashMap<String, String>();
				headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
				String url = configProperties.getSbUrl() + configProperties.getSbOrgSearchPath();

				Map<String, Object> apiResponse = (Map<String, Object>) outboundService.fetchResultUsingPost(url,
						orgSearchRequestBody, headers);
				if (Constants.OK.equalsIgnoreCase((String) apiResponse.get(Constants.RESPONSE_CODE))) {
					Map<String, Object> apiResponseResult = (Map<String, Object>) apiResponse.get(Constants.RESULT);
					Map<String, Object> resultResponse = (Map<String, Object>) apiResponseResult.get(Constants.RESPONSE);
					resultResponse.put(Constants.COUNT, totalOrgCount);
					response.put(Constants.RESPONSE, resultResponse);
				} else {
					response.getParams().setErrmsg("Failed to search org details");
					response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
				}
			} else {
				Map<String, Object> responseMap = new HashMap<String, Object>();
				responseMap.put(Constants.COUNT, 0);
				responseMap.put(Constants.CONTENT, Collections.EMPTY_LIST);
				response.put(Constants.RESPONSE, responseMap);
			}
		} catch (Exception e) {
			logger.error("Failed to search org details. Exception: ", e);
			response.getParams().setErrmsg(e.getMessage());
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return response;
	}

	private String validateOrgRequest(Map<String, Object> request) {
		List<String> params = new ArrayList<String>();
		StringBuilder strBuilder = new StringBuilder();
		Map<String, Object> requestData = (Map<String, Object>) request.get(Constants.REQUEST);
		if (ObjectUtils.isEmpty(requestData)) {
			strBuilder.append("Request object is empty.");
			return strBuilder.toString();
		}

		if (StringUtils.isEmpty((String) requestData.get(Constants.ORG_NAME))) {
			params.add(Constants.ORG_NAME);
		}

		String orgType = (String) requestData.get(Constants.ORGANIZATION_TYPE);
		if (StringUtils.isEmpty(((String) orgType))) {
			params.add(Constants.ORGANIZATION_TYPE);
		} else if (!Constants.STATE.equalsIgnoreCase(orgType) && !Constants.MINISTRY.equalsIgnoreCase(orgType)) {
			if (StringUtils.isEmpty((String) requestData.get(Constants.PARENT_MAP_ID))) {
				params.add(Constants.PARENT_MAP_ID);
			}
		}

		if (StringUtils.isEmpty((String) requestData.get(Constants.ORGANIZATION_SUB_TYPE))) {
			params.add(Constants.ORGANIZATION_SUB_TYPE);
		}

		if (ObjectUtils.isEmpty(requestData.get(Constants.IS_TENANT))) {
			params.add(Constants.IS_TENANT);
		}

		if (StringUtils.isEmpty((String) requestData.get(Constants.CHANNEL))) {
			params.add(Constants.CHANNEL);
		}

		if (!params.isEmpty()) {
			strBuilder.append("Invalid Request. Missing params - " + params);
		}

		return strBuilder.toString();
	}

	private String validateOrgSearchReq(Map<String, Object> requestData) {
		List<String> params = new ArrayList<String>();
		StringBuilder strBuilder = new StringBuilder();

		Map<String, Object> request = (Map<String, Object>) requestData.get(Constants.REQUEST);
		Map<String, Object> filters = (Map<String, Object>) request.get(Constants.FILTERS);
		if (ObjectUtils.isEmpty(filters)) {
			strBuilder.append("Filters in Request object is empty.");
			return strBuilder.toString();
		}

		if (StringUtils.isEmpty((String) filters.get(Constants.SB_ROOT_ORG_ID))) {
			params.add(Constants.SB_ROOT_ORG_ID);
		}

		if (!params.isEmpty()) {
			strBuilder.append("Invalid filters in Request. Missing params - " + params);
		}

		return strBuilder.toString();
	}

	private String checkOrgExist(String channel, String userToken) {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
		if (StringUtils.isNotEmpty(userToken)) {
			headers.put(Constants.X_AUTH_TOKEN, userToken);
		}
		Map<String, Object> filterMap = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put(Constants.CHANNEL, channel);
			}
		};
		Map<String, Object> searchRequest = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put(Constants.FILTERS, filterMap);
				put(Constants.FIELDS, Arrays.asList(Constants.CHANNEL, Constants.IDENTIFIER));
			}
		};
		Map<String, Object> searchRequestBody = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put(Constants.REQUEST, searchRequest);
			}
		};
		String url = configProperties.getSbUrl() + configProperties.getSbOrgSearchPath();
		Map<String, Object> apiResponse = (Map<String, Object>) outboundService.fetchResultUsingPost(url,
				searchRequestBody, headers);
		if (Constants.OK.equalsIgnoreCase((String) apiResponse.get(Constants.RESPONSE_CODE))) {
			Map<String, Object> result = (Map<String, Object>) apiResponse.get(Constants.RESULT);
			Map<String, Object> searchResponse = (Map<String, Object>) result.get(Constants.RESPONSE);
			int count = (int) searchResponse.get(Constants.COUNT);
			if (count > 0) {
				// The org is already exist - need to update the org details in org_hierarchy
				// table
				List<Map<String, Object>> orgList = (List<Map<String, Object>>) searchResponse.get(Constants.CONTENT);
				Map<String, Object> existingOrg = orgList.get(0);
				return (String) existingOrg.get(Constants.IDENTIFIER);
			}
		}
		return StringUtils.EMPTY;
	}

	private String createOrgInSunbird(Map<String, Object> request, String channel, String userToken) {
		String url = configProperties.getSbUrl() + configProperties.getLmsOrgCreatePath();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
		if (StringUtils.isNotEmpty(userToken)) {
			headers.put(Constants.X_AUTH_TOKEN, userToken);
		}

		Map<String, Object> apiResponse = (Map<String, Object>) outboundService.fetchResultUsingPost(url, request,
				headers);
		if (Constants.OK.equalsIgnoreCase((String) apiResponse.get(Constants.RESPONSE_CODE))) {
			Map<String, Object> result = (Map<String, Object>) apiResponse.get(Constants.RESULT);
			logger.info(String.format("Org onboarded successfully for Name: %s, with orgId: %s", channel,
					result.get(Constants.ORGANIZATION_ID)));
			return (String) result.get(Constants.ORGANIZATION_ID);
		}
		Map<String, Object> params = (Map<String, Object>) apiResponse.get(Constants.PARAMS);
		String errMsg = params != null ? (String) params.get(Constants.ERROR_MESSAGE) : Constants.ORG_CREATION_FAILED;
		logger.error("Org creation failed: " + errMsg);
		throw new RuntimeException(errMsg);
	}

	public Map<String, Object> getOrgDetails(List<String> orgIds, List<String> fields) {
		Map<String, Object> filters = new HashMap<>();
		filters.put(Constants.IDENTIFIER, orgIds);
		Map<String, Object> requestBody = new HashMap<>();
		requestBody.put(Constants.FILTERS, filters);
		requestBody.put(Constants.FIELDS, fields);
		Map<String, Object> request = new HashMap<>();
		request.put(Constants.REQUEST, requestBody);
		Map<String, String> headers = new HashMap<>();
		headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
		Map<String, Object> apiResponse = (Map<String, Object>) outboundService.fetchResultUsingPost(
				configProperties.getSbUrl() + configProperties.getSbOrgSearchPath(), request, headers);
		Map<String, Object> orgMap = new HashMap<>();
		if (Constants.OK.equalsIgnoreCase((String) apiResponse.get(Constants.RESPONSE_CODE))) {
			Map<String, Object> result = (Map<String, Object>) apiResponse.get(Constants.RESULT);
			if (MapUtils.isNotEmpty(result)) {
				Map<String, Object> response = (Map<String, Object>) result.get(Constants.RESPONSE);
				if (MapUtils.isNotEmpty(response)) {
					for (int i = 0; i < orgIds.size(); i++) {
						orgMap.put((String) response.get(orgIds.get(i)), response.get(Constants.CONTENT));
					}
				}
			}
		}
		return orgMap;
	}

	public void getOrgDetailsFromDB(List<String> orgIds, Map<String, String> orgInfoMap) {
		// This method is called from report tool.
		// Not doing anything for now.
	}

	public SBApiResponse createOrgForUserRegistration(Map<String, Object> request) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_EXT_CREATE);
		try {
			String errMsg = validateOrgRequestForRegistration(request);
			if (!StringUtils.isEmpty(errMsg)) {
				response.getParams().setErrmsg(errMsg);
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				return response;
			}

			Map<String, Object> requestData = (Map<String, Object>) request.get(Constants.REQUEST);

			boolean dbUpdateRequired = false;
			String orgId = checkOrgExist((String) requestData.get(Constants.CHANNEL), StringUtils.EMPTY);

			if (StringUtils.isEmpty(orgId)) {
				orgId = createOrgInSunbird(request, (String) requestData.get(Constants.CHANNEL), StringUtils.EMPTY);
				dbUpdateRequired = true;
			}

			if (!StringUtils.isEmpty(orgId)) {
				if (dbUpdateRequired) {
					OrgHierarchy existingDBRecord = orgRepository
							.findByChannel((String) requestData.get(Constants.CHANNEL));
					if (StringUtils.isBlank(existingDBRecord.getSbOrgId())) {
						existingDBRecord.setSbOrgId(orgId);
						if (StringUtils.isEmpty(existingDBRecord.getSbRootOrgId())) {
							existingDBRecord.setSbRootOrgId(fetchRootOrgId(existingDBRecord.getParentMapId()));
						}
						orgRepository.save(existingDBRecord);
					} else {
						logger.error(String.format(
								"Failed to update rootOrg details. RootOrg is already available in DB record. Existing: %s, NewValue: %s",
								existingDBRecord.getSbOrgId(), orgId));
					}
				}
				response.getResult().put(Constants.ORGANIZATION_ID, orgId);
				response.getResult().put(Constants.RESPONSE, Constants.SUCCESS);
			} else {
				response.getParams().setErrmsg("Failed to create organisation in Sunbird.");
				response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (Exception e) {
			logger.error("Failed to create org for user registration. Exception: ", e);
			response.getParams().setErrmsg(e.getMessage());
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return response;
	}

	private String createMapId(Map<String, Object> requestData) {
		List<OrgHierarchy> existingOrgList = null;
		String prefix = StringUtils.EMPTY;
		String mapIdNew = StringUtils.EMPTY;
		String orgType = (String) requestData.get(Constants.ORGANIZATION_TYPE);
		if (!Constants.STATE.equalsIgnoreCase(orgType) && !Constants.MINISTRY.equalsIgnoreCase(orgType)) {
			String orgSubType = (String) requestData.get(Constants.ORGANIZATION_SUB_TYPE);
			String parentMapId = (String) requestData.get(Constants.PARENT_MAP_ID);
			existingOrgList = orgRepository.findAllByParentMapId(parentMapId);
			if (Constants.DEPARTMENT.equalsIgnoreCase(orgSubType)) {
				prefix = "D_";
			} else if (Constants.BOARD.equalsIgnoreCase(orgSubType)) {
				prefix = "O_";
			} else if (Constants.TRAINING_INSTITUTE.equalsIgnoreCase(orgSubType)) {
				prefix = "T_";
			} else {
				prefix = "X_";
			}
			prefix = parentMapId + "_" + prefix;
		} else {
			existingOrgList = orgRepository.findAllBySbOrgType(orgType);
			if (Constants.STATE.equalsIgnoreCase(orgType)) {
				prefix = "S_";
			} else if (Constants.MINISTRY.equalsIgnoreCase(orgType)) {
				prefix = "M_";
			}
		}

		if (CollectionUtils.isNotEmpty(existingOrgList)) {
			List<String> mapIdList = new ArrayList<>();
			for (OrgHierarchy org : existingOrgList) {
				if (org.getMapId().startsWith(prefix)) {
					mapIdList.add(org.getMapId());
				}
			}
			if (Constants.ENABLED.equalsIgnoreCase(configProperties.getMapIdCounterEnabled())) {
				String finalPrefix = prefix;
				List<Integer> numbers = existingOrgList.stream()
						.map(OrgHierarchy::getMapId)                // Extract mapId
						.filter(mapId -> mapId.startsWith(finalPrefix)) // Filter by prefix
						.map(mapId -> {
							// Find all numeric parts and extract the last one
							Matcher matcher = Pattern.compile("\\d+").matcher(mapId);
							String lastNumber = null;
							while (matcher.find()) {
								lastNumber = matcher.group(); // Update with the current match
							}
							return lastNumber; // Return the last matched number
						})
						.filter(Objects::nonNull)                   // Exclude null values
						.map(Integer::parseInt)                     // Convert to integers
						.collect(Collectors.toList());             // Collect into a list

				// Find the maximum number or default to 0 if the list is empty
				int maxNumber = numbers.isEmpty() ? 0 : Collections.max(numbers);
				mapIdNew = prefix + (maxNumber + 1);
			}else{
				mapIdNew = prefix + (mapIdList.size() + 1);
			}
		} else {
			mapIdNew = prefix + "1";
		}
		return mapIdNew;
	}

	private String fetchRootOrgId(String mapId) {
		OrgHierarchy parentOrg = orgRepository.findByMapId(mapId);
		if (parentOrg != null && StringUtils.isBlank(parentOrg.getSbOrgId())) {
			// Let's try to create parent org
			createParentOrg(parentOrg);
			return parentOrg.getSbOrgId();
		}
		return StringUtils.EMPTY;
	}

	private void fetchMapIdFromDB(Map<String, Object> requestData) {
		List<OrgHierarchy> orgList = orgRepository.findAllByOrgName((String) requestData.get(Constants.ORG_NAME));
		if (ObjectUtils.isEmpty(orgList) || orgList.size() > 1) {
			// There are no args or multiple orgs. return from here.
			return;
		} else {
			// There is one org exist with the given name.
			// Otherwise this new dept name which already exist in someother ministry /
			// state / department.
			if (ObjectUtils.isEmpty(requestData.get(Constants.PARENT_MAP_ID))) {
				// ParentMapId is empty -- we are trying to create dept / state with same name.
				// Return simply.
				return;
			} else {
				OrgHierarchy existingOrg = orgList.get(0);
				// Check given parentMapId is same as existing record parentMapId.
				if (existingOrg.getParentMapId().equalsIgnoreCase((String) requestData.get(Constants.PARENT_MAP_ID))) {
					requestData.put(Constants.MAP_ID, existingOrg.getMapId());
				}
			}
		}
	}

	private String validateOrgRequestForRegistration(Map<String, Object> request) {
		List<String> params = new ArrayList<String>();
		StringBuilder strBuilder = new StringBuilder();
		Map<String, Object> requestData = (Map<String, Object>) request.get(Constants.REQUEST);
		if (ObjectUtils.isEmpty(requestData)) {
			strBuilder.append("Request object is empty.");
			return strBuilder.toString();
		}

		if (StringUtils.isBlank((String) requestData.get(Constants.CHANNEL))) {
			params.add(Constants.CHANNEL);
		}

		if (StringUtils.isEmpty((String) requestData.get(Constants.ORG_NAME))) {
			params.add(Constants.ORG_NAME);
		}

		if (StringUtils.isEmpty((String) requestData.get(Constants.MAP_ID))) {
			params.add(Constants.MAP_ID);
		}

		if (StringUtils.isEmpty((String) requestData.get(Constants.ORGANIZATION_TYPE))) {
			params.add(Constants.ORGANIZATION_TYPE);
		}

		if (StringUtils.isEmpty((String) requestData.get(Constants.ORGANIZATION_SUB_TYPE))) {
			params.add(Constants.ORGANIZATION_SUB_TYPE);
		}

		if (!params.isEmpty()) {
			strBuilder.append("Invalid Request. Missing params - " + params);
		}

		return strBuilder.toString();
	}

	public SBApiResponse orgExtSearchV2(Map<String, Object> request) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_HIERACHY_SEARCH);
		try {
			Map<String, Object> searchFilters = new HashMap<String, Object>();
			String errMsg = validateSearchRequest(request, searchFilters);
			if (StringUtils.isNotBlank(errMsg)) {
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				response.getParams().setErrmsg(errMsg);
				return response;
			}
			List<OrgHierarchyInfo> orgInfoList = new ArrayList<OrgHierarchyInfo>();
			List<OrgHierarchy> orgList = Collections.emptyList();
			if (searchFilters.containsKey(Constants.IDENTIFIER)) {
				orgList = orgRepository.findAllBySbOrgId((List<String>) searchFilters.get(Constants.IDENTIFIER));
			} else {
				orgList = orgRepository
						.searchOrgWithHierarchy((String) searchFilters.get(Constants.ORG_NAME));
			}
			Object parentTypeValue = searchFilters.get(Constants.PARENT_TYPE);
			List<String> parentTypeList = new ArrayList<String>();
			checkParentType(parentTypeList, parentTypeValue);
			if (CollectionUtils.isEmpty(orgList)) {
				orgList = Collections.emptyList();
			} else if (parentTypeList.isEmpty()) {
				for (OrgHierarchy org : orgList) {
					orgInfoList.add(org.toOrgInfo());
				}
			} else {
				Set<String> l1MapIdSet = orgList.stream().map(OrgHierarchy::getL1MapId)
						.filter(l1MapId -> Objects.nonNull(l1MapId)).collect(Collectors.toSet());

				List<OrgHierarchy> parentList = orgRepository.searchOrgForL1MapId(l1MapIdSet);
				Map<String, OrgHierarchy> parentListMap = parentList.stream()
						.collect(Collectors.toMap(OrgHierarchy::getMapId, orgHierarchy -> orgHierarchy));
				for (OrgHierarchy org : orgList) {
					OrgHierarchy parentObj = parentListMap.get(org.getL1MapId());
					if (parentObj != null) {
						// We found the parent for this orgObj.. check this parent's sbOrgType is given
						// parentType
						for (String parentType : parentTypeList) {
							if (parentType.equalsIgnoreCase(parentObj.getSbOrgType())) {
								orgInfoList.add(org.toOrgInfo());
							}
						}
					} else {
						// If Org doesn't have l1MapId then it could be State / Ministry
						for (String parentType : parentTypeList) {
							if (parentType.equalsIgnoreCase(org.getSbOrgType())) {
								orgInfoList.add(org.toOrgInfo());
							}
						}
					}
				}
			}
			int limit = (Integer) searchFilters.get(Constants.LIMIT);
			if (orgInfoList.size() > limit) {
				orgInfoList.subList(limit, orgInfoList.size()).clear();
			}

			response.getResult().put(Constants.COUNT, orgInfoList.size());
			response.getResult().put(Constants.RESPONSE, orgInfoList);
		} catch (Exception e) {
			logger.error("Failed to retrieve details from org hierarchy table. Exception: ", e);
		}
		return response;
	}

	private void checkParentType(List<String> parentTypeList, Object parentTypeValue) {
		if (parentTypeValue instanceof List) {
			List<?> valueList = (List<?>) parentTypeValue;
			if (valueList.stream().allMatch(String.class::isInstance)) {
				parentTypeList.addAll((List<String>) valueList);
			}
		} else if (parentTypeValue instanceof String) {
			parentTypeList.add((String) parentTypeValue);
		}
	}


	private String validateSearchRequest(Map<String, Object> request, Map<String, Object> searchFilters) {
		String errMsg = "";
		Map<String, Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
		if (ObjectUtils.isEmpty(requestBody)) {
			errMsg = Constants.INVALID_REQUEST;
			return errMsg;
		}
		Map<String, Object> filters = (Map<String, Object>) requestBody.get(Constants.FILTERS);
		if (ObjectUtils.isEmpty(filters)) {
			errMsg = Constants.INVALID_REQUEST;
			return errMsg;
		}

		boolean filterExist = false;
		if (filters.containsKey(Constants.IDENTIFIER)) {
			filterExist = true;
			List<String> orgList = (List<String>) filters.get(Constants.IDENTIFIER);
			if (CollectionUtils.isNotEmpty(orgList)) {
				searchFilters.put(Constants.IDENTIFIER, orgList);
			} else {
				errMsg = "Identifier list is empty";
			}
		}

		if (filters.containsKey(Constants.ORG_NAME) && filters.containsKey(Constants.PARENT_TYPE)) {
			filterExist = true;
			if (StringUtils.isNotBlank((String) filters.get(Constants.ORG_NAME))) {
				searchFilters.put(Constants.ORG_NAME, ((String) filters.get(Constants.ORG_NAME)).trim());
			} else {
				errMsg = "OrgName is empty in search request.";
			}

			Object parentTypeValue = filters.get(Constants.PARENT_TYPE);
			if (parentTypeValue instanceof List<?>) {
				List<?> parentTypeList = (List<?>) parentTypeValue;
				if (!parentTypeList.isEmpty() && !(parentTypeList.size() == 1 && "".equals(parentTypeList.get(0))) && parentTypeList.stream().allMatch(item -> item instanceof String)) {
					searchFilters.put(Constants.PARENT_TYPE, (List<String>) parentTypeList);
				} else {
					errMsg = "ParentType is empty in search request";
				}
			} else if (parentTypeValue instanceof String) {
				String parentTypeStr = ((String) parentTypeValue).trim();
				if (StringUtils.isNotBlank(parentTypeStr)) {
					searchFilters.put(Constants.PARENT_TYPE, parentTypeStr);
				} else {
					errMsg = "ParentType is empty in search request";
				}
			} else {
				errMsg = "Invalid data type for ParentType";
			}
		}

		if (!filterExist) {
			errMsg = "Need identifier OR orgName and parentType in Filters";
		}
		Integer limit = (Integer) requestBody.get(Constants.LIMIT);
		if (limit == null) {
			searchFilters.put(Constants.LIMIT, configProperties.getOrgSearchResponseDefaultLimit());
		} else {
			searchFilters.put(Constants.LIMIT, limit);
		}
		return errMsg;
	}

	private String prepareChannelName(String parentMapId, Map<String, Object> requestData) {
		String channelName = "";
		if (StringUtils.isBlank(parentMapId)) {
			return channelName;
		}
		List<OrgHierarchy> parentList = orgRepository.findAllByMapId(parentMapId);
		if (!ObjectUtils.isEmpty(parentList) && parentList.size() > 0) {
			OrgHierarchy parent = parentList.get(0);
			if (!Constants.SPV.equalsIgnoreCase(parent.getParentMapId())) {
				prepareChannelName(parent.getParentMapId(), requestData);
				requestData.put(Constants.L2_MAP_ID, parent.getMapId());
				requestData.put(Constants.L2_ORG_NAME, parent.getOrgName());
			} else {
				requestData.put(Constants.L1_MAP_ID, parent.getMapId());
				requestData.put(Constants.L1_ORG_NAME, parent.getOrgName());
			}
			channelName = parent.getChannel() + configProperties.getOrgChannelDelimitter();
		}
		return channelName;
	}

	private OrgHierarchy getOrgRecord(Map<String, Object> request, OrgHierarchy newOrg) {
		newOrg.setOrgCode((String) request.get(Constants.MAP_ID));
		newOrg.setSbOrgId((String) request.get(Constants.SB_ORG_ID));
		newOrg.setSbOrgType((String) request.get(Constants.SB_ORG_TYPE));
		newOrg.setSbOrgSubType((String) request.get(Constants.SB_SUB_ORG_TYPE));
		newOrg.setSbRootOrgId((String) request.get(Constants.SB_ROOT_ORG_ID));
		newOrg.setL1MapId((String) request.get(Constants.L1_MAP_ID));
		newOrg.setL1OrgName((String) request.get(Constants.L1_ORG_NAME));
		newOrg.setL2MapId((String) request.get(Constants.L2_MAP_ID));
		newOrg.setL2OrgName((String) request.get(Constants.L2_ORG_NAME));
		return newOrg;
	}

	public SBApiResponse createParentOrg(OrgHierarchy parentOrg) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_EXT_CREATE);
		try {

			String orgId = checkOrgExist(parentOrg.getChannel(), StringUtils.EMPTY);

			if (StringUtils.isEmpty(orgId)) {
				Map<String, Object> request = new HashMap<String, Object>();
				Map<String, Object> requestBody = new HashMap<String, Object>();

				requestBody.put(Constants.ORG_NAME, parentOrg.getOrgName());
				requestBody.put(Constants.CHANNEL, parentOrg.getChannel());
				requestBody.put(Constants.IS_TENANT, true);
				requestBody.put(Constants.ORGANIZATION_TYPE, parentOrg.getSbOrgType());
				requestBody.put(Constants.ORGANIZATION_SUB_TYPE, parentOrg.getSbOrgSubType());
				request.put(Constants.REQUEST, requestBody);
				orgId = createOrgInSunbird(request, parentOrg.getChannel(), StringUtils.EMPTY);
			}

			if (!StringUtils.isEmpty(orgId)) {
				String sbRootOrgId = orgRepository.getSbOrgIdFromMapId(parentOrg.getParentMapId());
				;
				if (StringUtils.isBlank(parentOrg.getSbRootOrgId())) {
					sbRootOrgId = orgRepository.getSbOrgIdFromMapId(parentOrg.getParentMapId());
				}
				if (StringUtils.isBlank(parentOrg.getSbRootOrgId()) && !StringUtils.isEmpty(sbRootOrgId)) {
					orgRepository.updateSbOrgIdAndSbOrgRootIdForChannel(parentOrg.getChannel(),
							orgId, sbRootOrgId);
				} else {
					orgRepository.updateOrgIdForChannel(parentOrg.getChannel(), orgId);
				}
				response.getResult().put(Constants.ORGANIZATION_ID, orgId);
				response.getResult().put(Constants.RESPONSE, Constants.SUCCESS);
				parentOrg.setSbOrgId(orgId);
			} else {
				response.getParams().setErrmsg("Failed to create parent organisation in Sunbird.");
				response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (Exception e) {
			logger.error("Failed to create parent org. Exception: ", e);
			response.getParams().setErrmsg(e.getMessage());
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return response;
	}

	@Override
	public SBApiResponse listAllOrg(String parentMapId) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_V2_LIST);
		if (StringUtils.isEmpty(parentMapId)) {
			parentMapId = Constants.SPV;
		}

		List<OrgHierarchy> orgHierarchyList = null;
		if (Constants.MINISTRY.equalsIgnoreCase(parentMapId)
				|| Constants.STATE.equalsIgnoreCase(parentMapId)) {
			orgHierarchyList = orgRepository.findAllBySbOrgType(parentMapId);
		} else {
			orgHierarchyList = orgRepository.findAllOrgByParentMapId(parentMapId);
		}

		if (CollectionUtils.isNotEmpty(orgHierarchyList)) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			responseMap.put(Constants.CONTENT, orgHierarchyList);
			responseMap.put(Constants.COUNT, orgHierarchyList.size());
			response.put(Constants.RESPONSE, responseMap);
		} else {
			Map<String, Object> responseMap = new HashMap<>();
			responseMap.put(Constants.CONTENT, orgHierarchyList);
			responseMap.put(Constants.COUNT, orgHierarchyList.size());
			response.put(Constants.RESPONSE, responseMap);
			response.getParams().setErrmsg("No child org found for Id: " + parentMapId);
		}

		return response;
	}

	/**
	 * Updates the organization details for the given organization ID.
	 *
	 * @param orgRequest the organization request data
	 * @param userToken  the user authentication token
	 * @return the API response object
	 */
	@Override
	public SBApiResponse update(Map<String, Object> orgRequest, String userToken) {
		logger.info("ExtendedOrgServiceImpl::update::Starting the update of the organization");
		SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.API_ORG_EXT_UPDATE);
		String errMsg = validateRequestFields(orgRequest, outgoingResponse);
		if (!StringUtils.isEmpty(errMsg)) return outgoingResponse;
		List<OrgHierarchy> orgHierarchyList = orgRepository.findAllBySbOrgId(Collections.singletonList((String) orgRequest.get(Constants.ORG_ID)));
		String sborgsubtype = "";
		if (CollectionUtils.isNotEmpty(orgHierarchyList) && orgHierarchyList.get(0) != null) {
			sborgsubtype = orgHierarchyList.get(0).getSbOrgSubType();
			logger.info("ExtendedOrgServiceImpl::update::SbOrgType: " + sborgsubtype + " for the organization" + orgRequest.get(Constants.ORG_ID));
		}
		if (Constants.BOARD.equalsIgnoreCase(sborgsubtype)) {
			logger.info("ExtendedOrgServiceImpl::update:: Updating the board details for organization:" + orgRequest.get(Constants.ORG_ID));
			orgRepository.updateOrgNameBySbOrgId((String) orgRequest.get(Constants.ORG_ID), (String) orgRequest.get(Constants.ORG_NAME));
			OrgHierarchyInfo orgHierarchyInfo = new OrgHierarchyInfo();
			orgHierarchyInfo.setOrgName((String) orgRequest.get(Constants.ORG_NAME));
			orgHierarchyInfo.setSbOrgId((String) orgRequest.get(Constants.ORG_ID));
			Map<String, Object> orgDataUpdateResonse = updateOrgDetailsToDB(userToken, orgHierarchyInfo, orgRequest);
			if (MapUtils.isEmpty(orgDataUpdateResonse) || !orgDataUpdateResonse.get(Constants.RESPONSE_CODE).equals(Constants.OK)) {
				logger.info("ExtendedOrgServiceImpl::update::Failed to update Org details for organization: " + orgHierarchyInfo.getSbOrgId());
				setInternalServerError(outgoingResponse, "Error while updating the organization details");
			} else {
				populateSuccessResponse(outgoingResponse);
			}
		} else {
			Map<String, Object> result = new HashMap<>();
			logger.info("ExtendedOrgServiceImpl::update:: SbOrgType is not 'board' for the organization:" + orgRequest.get(Constants.ORG_ID));
			result.put(Constants.SB_SUB_ORG_TYPE, "Updating ministry,state or department is not allowed");
			outgoingResponse.getResult().putAll(result);
			outgoingResponse.getParams().setStatus(Constants.OK);
			outgoingResponse.setResponseCode(HttpStatus.OK);
		}
		return outgoingResponse;
	}

	/**
	 * Validates specific fields in the request and updates the API response accordingly.
	 *
	 * @param request  The request object.
	 * @param response The API response object.
	 * @return An error message if any required field is invalid, otherwise an empty string.
	 */
	private String validateRequestFields(Map<String, Object> request, SBApiResponse response) {
		if (StringUtils.isBlank(MapUtils.getString(request, Constants.ORG_ID))) {
			response.getParams().setStatus(Constants.FAILED);
			response.getParams().setErrmsg("Organization ID is missing");
			response.setResponseCode(HttpStatus.BAD_REQUEST);
			return "Organization ID is missing";
		}
		if (StringUtils.isBlank(MapUtils.getString(request, Constants.ORG_NAME))) {
			response.getParams().setStatus(Constants.FAILED);
			response.getParams().setErrmsg("Organization name is missing");
			response.setResponseCode(HttpStatus.BAD_REQUEST);
			return "Organization name is missing";
		}
		return "";
	}


	/**
	 * Updates the organization details in the database using the provided request data.
	 *
	 * @param authUserToken    the authentication token for the user making the request
	 * @param orgHierarchyInfo the organization hierarchy information
	 * @param orgRequest       the organization request data
	 * @return the result of the update operation
	 */
	private Map<String, Object> updateOrgDetailsToDB(String authUserToken, OrgHierarchyInfo orgHierarchyInfo, Map<String, Object> orgRequest) {
		logger.info("ExtendedOrgServiceImpl::updateOrgDetailsToDB:Updating the Org details for the organization." + orgHierarchyInfo.getSbOrgId());
		Map<String, Object> request = new HashMap<>();
		Map<String, Object> updateRequest = new HashMap<>();
		Map<String, String> headerValues = new HashMap<>();
		headerValues.put(Constants.X_AUTH_TOKEN, authUserToken);
		request.put(Constants.ORGANIZATION_ID, orgHierarchyInfo.getSbOrgId());
		if (MapUtils.getObject(orgRequest, Constants.ORG_NAME) != null) {
			request.put(Constants.ORG_NAME, orgRequest.get(Constants.ORG_NAME));
		}
		if (MapUtils.getObject(orgRequest, Constants.DESCRIPTION) != null) {
			request.put(Constants.DESCRIPTION, orgRequest.get(Constants.DESCRIPTION));
		}
		if (MapUtils.getObject(orgRequest, Constants.LOGO) != null) {
			request.put(Constants.LOGO, orgRequest.get(Constants.LOGO));
		}
		if (MapUtils.getObject(orgRequest, Constants.PARENT_PATH_ID) != null) {
			request.put(Constants.PARENT_PATH_ID, orgRequest.get(Constants.PARENT_PATH_ID));
		}
		updateRequest.put(Constants.REQUEST, request);
		return outboundService.fetchResultUsingPatch(
				configProperties.getSbUrl() + configProperties.getUpdateOrgPath(), updateRequest, headerValues);
	}

	/**
	 * Sets the internal server error response for a given API response object.
	 *
	 * @param response the API response object to update
	 * @param errorMsg the error message to include in the response
	 */
	private void setInternalServerError(SBApiResponse response, String errorMsg) {
		response.getParams().setStatus(HttpStatus.INTERNAL_SERVER_ERROR.toString());
		response.getParams().setErrmsg(errorMsg);
		response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
	}

	/**
	 * Populates the success response object with the result of the organization ID update operation.
	 *
	 * @param response the API response object to be populated
	 */
	private void populateSuccessResponse(SBApiResponse response) {
		Map<String, Object> result = new HashMap<>();
		result.put(Constants.ORGANIZATION_ID, "Organisation Id Updated successfully");
		response.getResult().putAll(result);
		response.getParams().setStatus(Constants.OK);
		response.setResponseCode(HttpStatus.OK);
	}

	/**
	 * Validates the request fields for organisation creation.
	 *
	 * @param request the request map containing the organisation creation data
	 * @param response the API response object
	 * @return an error message if validation fails, otherwise an empty string
	 */
	private String validateRequestFieldsOrganisationCreate(Map<String, Object> request, SBApiResponse response) {
		Map<String,Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
		if (StringUtils.isBlank(MapUtils.getString(requestBody, Constants.PARENT_MAP_ID)) && Constants.BOARD.equalsIgnoreCase(MapUtils.getString(requestBody, Constants.ORGANIZATION_SUB_TYPE))) {
			response.getParams().setStatus(Constants.FAILED);
			response.getParams().setErrmsg("Parent Map ID is Empty/missing");
			response.setResponseCode(HttpStatus.BAD_REQUEST);
			return "Parent Map ID is missing";
		}
		return "";
	}

	/**
	 * This method fetches and adds state or ministry details to the given request map based on the provided parent map ID.
	 * It utilizes the organization repository to retrieve hierarchical organizational details.
	 *
	 * @param requestBody The request map containing the parent map ID and other details.
	 */
	private void fetchStateOrMinistryDetails(Map<String, Object> requestBody) {
		Map<String,Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
		if (Constants.BOARD.equalsIgnoreCase((String) request.get(Constants.ORGANIZATION_SUB_TYPE))) {
			logger.info("ExtendedOrgServiceImpl::fetchStateOrMinistryDetails:Fetching the state or ministry details based on the parent map ID." + request.get("parentMapId"));
			OrgHierarchy deptOrgDetails = orgRepository.findByMapId((String) request.get(Constants.PARENT_MAP_ID));
			if (StringUtils.isNotEmpty(deptOrgDetails.getOrgName()) && "department".equalsIgnoreCase(deptOrgDetails.getSbOrgSubType())) {
				logger.info("ExtendedOrgServiceImpl::fetchStateOrMinistryDetails: Found department details. " +
						"DeptName: {}, ParentMapId: {}", deptOrgDetails.getOrgName(), deptOrgDetails.getParentMapId());
				request.put(Constants.DEPT_NAME, deptOrgDetails.getOrgName());
				String deptMapId = deptOrgDetails.getParentMapId();
				OrgHierarchy ministryOrgDetails = orgRepository.findByMapId(deptMapId);
				if (StringUtils.isNotEmpty(deptOrgDetails.getOrgName()) &&
						(Constants.MINISTRY.equalsIgnoreCase(ministryOrgDetails.getSbOrgType()) || Constants.STATE.equalsIgnoreCase(ministryOrgDetails.getSbOrgType()))) {
					logger.info("ExtendedOrgServiceImpl::fetchStateOrMinistryDetails: Found ministry/state details. " +
							"Name: {}, Type: {}", ministryOrgDetails.getOrgName(), ministryOrgDetails.getSbOrgType());
					request.put(Constants.MINISTRY_STATE_NAME, ministryOrgDetails.getOrgName());
					request.put(Constants.MINISTRY_STATE_TYPE, ministryOrgDetails.getSbOrgType());
				}
			} else if (StringUtils.isNotEmpty(deptOrgDetails.getOrgName()) &&
					(Constants.MINISTRY.equalsIgnoreCase(deptOrgDetails.getSbOrgType()) || Constants.STATE.equalsIgnoreCase(deptOrgDetails.getSbOrgType()))) {
				request.put(Constants.MINISTRY_STATE_NAME, deptOrgDetails.getOrgName());
				request.put(Constants.MINISTRY_STATE_TYPE, deptOrgDetails.getSbOrgType());
			}
		}
	}

	@Override
	public SBApiResponse updateV2(Map<String, Object> orgRequest, String userToken) {
		logger.info("ExtendedOrgServiceImpl::updateV2::Starting the update of the organization");
		SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.API_ORG_EXT_UPDATE);
		String errMsg = validateRequestFieldsV2(orgRequest, outgoingResponse);
		if (!StringUtils.isEmpty(errMsg)) return outgoingResponse;
		OrgHierarchyInfo orgHierarchyInfo = new OrgHierarchyInfo();
		if(!StringUtils.isBlank(MapUtils.getString(orgRequest, Constants.ORG_NAME))){
			orgRepository.updateOrgNameBySbOrgId((String) orgRequest.get(Constants.ORG_ID), (String) orgRequest.get(Constants.ORG_NAME));
			orgHierarchyInfo.setOrgName((String) orgRequest.get(Constants.ORG_NAME));
		}
		orgHierarchyInfo.setSbOrgId((String) orgRequest.get(Constants.ORG_ID));
		Map<String, Object> orgDataUpdateResonse = updateOrgDetailsToDB(userToken, orgHierarchyInfo, orgRequest);
		if (MapUtils.isEmpty(orgDataUpdateResonse) || !Constants.OK.equals(orgDataUpdateResonse.get(Constants.RESPONSE_CODE))) {
			logger.info("ExtendedOrgServiceImpl::updateV2::Failed to update Org details for organization: " + orgHierarchyInfo.getSbOrgId());
			setInternalServerError(outgoingResponse, "Error while updating the organization details");
		} else {
			populateSuccessResponse(outgoingResponse);
		}
		return outgoingResponse;
	}

	private String validateRequestFieldsV2(Map<String, Object> request, SBApiResponse response) {
		String errMsg = "";
		String orgId = MapUtils.getString(request, Constants.ORG_ID);
		if (StringUtils.isBlank(orgId)) {
			errMsg = "Organization ID is missing";
			response.getParams().setStatus(Constants.FAILED);
			response.getParams().setErrmsg(errMsg);
			response.setResponseCode(HttpStatus.BAD_REQUEST);
			return errMsg;
		}
		for (String fieldName : request.keySet()) {
			if (Constants.ORG_ID.equals(fieldName)) {
				continue;
			}
			if (!configProperties.getOrgUpdatableFields().contains(fieldName)) {
				errMsg = "Field : " + fieldName + " is not allowed to be updated.";
				logger.info("ExtendedOrgServiceImpl::updateV2::Invalid field in request: {}", fieldName);
				response.getParams().setStatus(Constants.FAILED);
				response.getParams().setErrmsg(errMsg);
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				return errMsg;
			}
			logger.info("Updating organisation for field: {}", fieldName);
		}
		return errMsg;
	}

	@Override
	public SBApiResponse getNodalOfficer(String orgId) {
		logger.info("Fetching MDO Leader List");
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_LIST);
		try {
			// VALIDATION
			if (StringUtils.isEmpty(orgId)) {
				response.getParams().setErrmsg("orgId is required");
				response.getParams().setStatus(Constants.FAILED);
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				return response;
			}
			List<Map<String, Object>> contentList = new ArrayList<>();
			BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
			// FILTER BY ORG ID
			finalQuery.must(QueryBuilders.termQuery(Constants.ROOT_ORG_ID, orgId));
			finalQuery.must(QueryBuilders.termQuery("roles.role", Constants.MDO_LEADER));
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(finalQuery);
			sourceBuilder.size(userSearchLimit);
			// USER PROFILE SEARCH
			SearchResponse searchResponse = indexerService.getEsResult(serverConfig.getSbEsUserProfileIndex(), serverConfig.getEsProfileIndexType(), sourceBuilder, ProjectUtil.ESIndexType.USER_ES);
			Map<String, Map<String, Object>> userMap = new HashMap<>();
			List<String> rootOrgIds = new ArrayList<>();

			/* ---------- USER LOOP ---------- */
			for (SearchHit hit : searchResponse.getHits()) {
				Map<String, Object> source = hit.getSourceAsMap();
				String rootOrgId = (String) source.get(Constants.ROOT_ORG_ID);
				userMap.put(rootOrgId, source);
				rootOrgIds.add(rootOrgId);
			}

			/* ---------- BATCH LOGIC ---------- */
			int batchSize = this.batchsize;
			Map<String, String> ministryMap = new HashMap<>();
			for (int i = 0; i < rootOrgIds.size(); i += batchSize) {
				List<String> batch = rootOrgIds.subList(i, Math.min(i + batchSize, rootOrgIds.size()));
				/* ---------- ORG SEARCH REQUEST ---------- */
				Map<String, Object> orgSearchRequestBody = new HashMap<>();
				Map<String, Object> request = new HashMap<>();
				Map<String, Object> filters = new HashMap<>();
				filters.put(Constants.ID, batch);
				request.put(Constants.FILTERS, filters);
				request.put(Constants.LIMIT, batch.size());
				orgSearchRequestBody.put(Constants.REQUEST, request);
				/* ---------- HEADERS ---------- */
				Map<String, String> headers = new HashMap<>();
				headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
				/* ---------- URL ---------- */
				String url = configProperties.getSbUrl() + configProperties.getSbOrgSearchPath();
				/* ---------- MICROSERVICE CALL ---------- */
				Map<String, Object> apiResponse = (Map<String, Object>) outboundService.fetchResultUsingPost(url, orgSearchRequestBody, headers);
				Map<String, Object> result = (Map<String, Object>) apiResponse.get(Constants.RESULT);
				if (result != null) {
					Map<String, Object> responseMap = (Map<String, Object>) result.get(Constants.RESPONSE);
					if (responseMap != null) {
						List<Map<String, Object>> orgContent = (List<Map<String, Object>>) responseMap.get(Constants.CONTENT);
						if (CollectionUtils.isNotEmpty(orgContent)) {
							for (Map<String, Object> org : orgContent) {
								String identifier = org.get(Constants.IDENTIFIER) != null
										? (String) org.get(Constants.IDENTIFIER)
										: "";
								String ministry = org.get(Constants.MINISTRY_STATE_NAME) != null
										? (String) org.get(Constants.MINISTRY_STATE_NAME)
										: "";
								ministryMap.put(identifier, ministry);
							}
						}
					}
				}
			}

			/* ---------- FINAL USER LOOP ---------- */
			if (CollectionUtils.isNotEmpty(rootOrgIds)) {
				for (String rootOrgId : rootOrgIds) {
					Map<String, Object> source = userMap.get(rootOrgId);
					if (MapUtils.isNotEmpty(source)) {
						String orgName = (String) source.get(Constants.ROOT_ORG_NAME);
						String ministry = ministryMap.get(rootOrgId);
						String nodalName = null;
						String email = null;
						Map<String, Object> profileDetails = (Map<String, Object>) source.get(Constants.PROFILE_DETAILS);
						if (MapUtils.isNotEmpty(profileDetails)) {
							Map<String, Object> personalDetails = (Map<String, Object>) profileDetails.get(Constants.PERSONAL_DETAILS);
							if (MapUtils.isNotEmpty(personalDetails)) {
								nodalName = (String) personalDetails.get(Constants.FIRST_NAME_LOWER_CASE);
								email = (String) personalDetails.get(Constants.PRIMARY_EMAIL);
								if (email != null) {
									email = email.replace("@", "[at]");
									email = email.replace(".", "[dot]");
								}
							}
						}
						Map<String, Object> leaderMap = new LinkedHashMap<>();
						// DOC RESPONSE FORMAT
						leaderMap.put("Name", nodalName);
						leaderMap.put("OrgName", orgName);
						leaderMap.put("StateOrMinistryName", ministry);
						leaderMap.put("Email", email);
						contentList.add(leaderMap);
					}
				}
			}

			/* ---------- FINAL RESPONSE ---------- */
			Map<String, Object> finalResponse = new HashMap<>();
			finalResponse.put(Constants.CONTENT, contentList);
			response.getResult().put(Constants.RESPONSE, finalResponse);
			response.getParams().setStatus(Constants.SUCCESS);
		} catch (Exception e) {
			logger.error("Error while fetching MDO Leader List", e);
			response.getParams().setStatus(Constants.FAILED);
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return response;
	}

}
