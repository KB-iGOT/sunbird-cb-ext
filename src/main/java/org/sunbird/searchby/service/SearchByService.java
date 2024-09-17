package org.sunbird.searchby.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.model.FracApiResponse;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.logger.CbExtLogger;
import org.sunbird.searchby.model.CompetencyInfo;
import org.sunbird.searchby.model.FracCommonInfo;
import org.sunbird.searchby.model.ProviderInfo;
import org.sunbird.workallocation.model.FracStatusInfo;

import java.util.*;

@Service
@SuppressWarnings("unchecked")
public class SearchByService {

	private CbExtLogger logger = new CbExtLogger(getClass().getName());

	@Autowired
	CbExtServerProperties cbExtServerProperties;

	@Autowired
	RedisCacheMgr redisCacheMgr;

	@Autowired
	ObjectMapper mapper;

	@Autowired
	OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

	@Autowired
	AccessTokenValidator accessTokenValidator;

	public Collection<CompetencyInfo> getCompetencyDetails(String authUserToken) throws Exception {
		String strCompetencyMap = redisCacheMgr.getCache(Constants.COMPETENCY_CACHE_NAME);
		Map<String, CompetencyInfo> competencyMap = new HashMap<>();
		if (!StringUtils.isEmpty(strCompetencyMap)) {
			competencyMap = mapper.readValue(strCompetencyMap, new TypeReference<Map<String, CompetencyInfo>>() {
			});
		}

		if (CollectionUtils.isEmpty(competencyMap)) {
			logger.info("Initializing/Refreshing the Cache Value for Key : " + Constants.COMPETENCY_CACHE_NAME);
			competencyMap = updateCompetencyDetails(authUserToken);
		}
		return competencyMap.values();
	}

	public Collection<ProviderInfo> getProviderDetails(String authUserToken) throws Exception {
		String strProviderInfo = redisCacheMgr.getCache(Constants.PROVIDER_CACHE_NAME);
		Map<String, ProviderInfo> providerMap = new HashMap<>();
		if (!StringUtils.isEmpty(strProviderInfo)) {
			providerMap = mapper.readValue(strProviderInfo, new TypeReference<Map<String, ProviderInfo>>() {
			});
		}

		if (CollectionUtils.isEmpty(providerMap)) {
			logger.info("Initializing/Refreshing the Cache Value for Key : " + Constants.PROVIDER_CACHE_NAME);
			providerMap = updateProviderDetails(authUserToken);
		}
		return providerMap.values();
	}

	public FracApiResponse listPositions(String userToken) {
		FracApiResponse response = new FracApiResponse();
		response.setStatusInfo(new FracStatusInfo());
		response.getStatusInfo().setStatusCode(HttpStatus.OK.value());
		try {
			Map<String, List<FracCommonInfo>> positionMap = new HashMap<>();
			String strPositionMap = redisCacheMgr.getCache(Constants.POSITIONS_CACHE_NAME);
			if (!StringUtils.isEmpty(strPositionMap)) {
				positionMap = mapper.readValue(strPositionMap, new TypeReference<Map<String, List<FracCommonInfo>>>() {
				});
			}

			if (ObjectUtils.isEmpty(positionMap)
					|| CollectionUtils.isEmpty(positionMap.get(Constants.POSITIONS_CACHE_NAME))) {
				logger.info("Initializing / Refreshing the Cache value for key : " + Constants.POSITIONS_CACHE_NAME);
				try {
					positionMap = updateDesignationDetails(userToken);
					response.setResponseData(positionMap.get(Constants.POSITIONS_CACHE_NAME));
				} catch (Exception e) {
					logger.error(e);
					response.getStatusInfo().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
					response.getStatusInfo().setErrorMessage(e.getMessage());
				}
			} else {
				response.setResponseData(positionMap.get(Constants.POSITIONS_CACHE_NAME));
			}
		} catch (Exception e) {
			response.getStatusInfo().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
		}

		return response;
	}

	private Map<String, CompetencyInfo> updateCompetencyDetails(String authUserToken) throws Exception {
		Map<String, CompetencyInfo> competencyMap;
		Map<String, List<CompetencyInfo>> comInfoByType = new HashMap<>();
		Map<String, List<CompetencyInfo>> comInfoByArea = new HashMap<>();

		// Get facets from Composite Search
		Map<String, String> headers = new HashMap<>();
		headers.put(Constants.USER_TOKEN, authUserToken);
		headers.put(Constants.AUTHORIZATION, cbExtServerProperties.getSbApiKey());

		HashMap<String, Object> reqBody = new HashMap<>();
		HashMap<String, Object> req = new HashMap<>();
		req.put(Constants.FACETS, Arrays.asList(Constants.COMPETENCY_FACET_NAME));
		Map<String, Object> filters = new HashMap<>();
		filters.put(Constants.PRIMARY_CATEGORY, Arrays.asList(Constants.COURSE, Constants.PROGRAM));
		filters.put(Constants.STATUS, Arrays.asList(Constants.LIVE));
		req.put(Constants.FILTERS, filters);
		req.put(Constants.LIMIT, 0);
		reqBody.put(Constants.REQUEST, req);

		Map<String, Object> compositeSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
				cbExtServerProperties.getKmBaseHost() + cbExtServerProperties.getKmCompositeSearchPath(), reqBody,
				headers);

		Map<String, Object> compositeSearchResult = (Map<String, Object>) compositeSearchRes.get(Constants.RESULT);
		List<Map<String, Object>> facetsList = (List<Map<String, Object>>) compositeSearchResult.get(Constants.FACETS);
		if (!CollectionUtils.isEmpty(facetsList)) {
			competencyMap = new HashMap<>();
			for (Map<String, Object> facetObj : facetsList) {
				String name = (String) facetObj.get(Constants.NAME);
				if (Constants.COMPETENCY_FACET_NAME.equals(name)) {
					List<Map<String, Object>> facetValueList = (List<Map<String, Object>>) facetObj
							.get(Constants.VALUES);
					if (!CollectionUtils.isEmpty(facetValueList)) {
						for (Map<String, Object> facetValueObj : facetValueList) {
							CompetencyInfo compInfo = new CompetencyInfo();
							// TODO - Make sure which competency field is unique
							compInfo.setName((String) facetValueObj.get(Constants.NAME));
							compInfo.setContentCount((int) facetValueObj.get(Constants.COUNT));
							competencyMap.put((String) facetValueObj.get(Constants.NAME), compInfo);
						}
					}
				}
			}
		} else {
			Exception err = new Exception("Failed to get facets value from Composite Search API.");
			logger.error(err);
			try {
				logger.info("Received Response: " + (new ObjectMapper()).writeValueAsString(compositeSearchResult));
			} catch (Exception e) {
			}
			throw err;
		}

		// Get Competency Values
		headers = new HashMap<>();
		headers.put(Constants.AUTHORIZATION, Constants.BEARER + authUserToken);
		reqBody = new HashMap<>();
		List<Map<String, Object>> searchList = new ArrayList<>();

		for (String compName : competencyMap.keySet()) {
			Map<String, Object> compSearchObj = new HashMap<>();
			compSearchObj.put(Constants.TYPE, Constants.COMPETENCY.toUpperCase());
			compSearchObj.put(Constants.FIELD, Constants.NAME);
			compSearchObj.put(Constants.KEYWORD, compName);
			searchList.add(compSearchObj);
		}
		reqBody.put(Constants.SEARCHES, searchList);
		reqBody.put(Constants.CHILD_COUNT, false);
		reqBody.put(Constants.CHILD_NODES, false);

		Map<String, Object> fracSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
				cbExtServerProperties.getFracHost() + cbExtServerProperties.getFracSearchPath(), reqBody, headers);

		List<Map<String, Object>> fracResponseList = (List<Map<String, Object>>) fracSearchRes
				.get(Constants.RESPONSE_DATA);

		if (!CollectionUtils.isEmpty(fracResponseList)) {
			for (Map<String, Object> respObj : fracResponseList) {
				String compName = ((String) respObj.get(Constants.NAME)).toLowerCase();
				if (competencyMap.containsKey(compName)) {
					CompetencyInfo compInfo = competencyMap.get(compName);
					compInfo.setName((String) respObj.get(Constants.NAME));
					compInfo.setCompetencyArea(((Map<String, String>) respObj.get(Constants.ADDITIONAL_PROPERTIES))
							.get(Constants.COMPETENCY_AREA));
					compInfo.setCompetencyType(((Map<String, String>) respObj.get(Constants.ADDITIONAL_PROPERTIES))
							.get(Constants.COMPETENCY_TYPE));
					compInfo.setDescription((String) respObj.get(Constants.DESCRIPTION));
					compInfo.setId((String) respObj.get(Constants.ID));
					compInfo.setSource((String) respObj.get(Constants.SOURCE));
					compInfo.setStatus((String) respObj.get(Constants.STATUS));
					competencyMap.put(compName, compInfo);

//					// Competency Map by Type
					if (!compInfo.getCompetencyType().isEmpty()) {
						String competencyType = compInfo.getCompetencyType();
						List<CompetencyInfo> competencyInfoList;
						if (comInfoByType.containsKey(competencyType)) {
							competencyInfoList = comInfoByType.get(competencyType);
						} else {
							competencyInfoList = new ArrayList<>();
						}
						competencyInfoList.add(compInfo);
						comInfoByType.put(competencyType, competencyInfoList);
					}
//					// Competency Map by Area
					if (!compInfo.getCompetencyArea().isEmpty()) {
						String competencyArea = compInfo.getCompetencyArea();
						List<CompetencyInfo> competencyInfoList;
						if (comInfoByArea.containsKey(competencyArea)) {
							competencyInfoList = comInfoByArea.get(competencyArea);
						} else {
							competencyInfoList = new ArrayList<>();
						}
						competencyInfoList.add(compInfo);
						comInfoByArea.put(competencyArea, competencyInfoList);
					}
				}

			}
		} else {
			Exception err = new Exception("Failed to get competency info from FRAC API.");
			logger.error(err);
			try {
				logger.info("Received Response: " + (new ObjectMapper()).writeValueAsString(fracSearchRes));
			} catch (Exception e) {
			}
			throw err;
		}

		redisCacheMgr.putCache(Constants.COMPETENCY_CACHE_NAME, competencyMap);
		redisCacheMgr.putCache(Constants.COMPETENCY_CACHE_NAME_BY_TYPE, comInfoByType);
		redisCacheMgr.putCache(Constants.COMPETENCY_CACHE_NAME_BY_AREA, comInfoByArea);

		return competencyMap;
	}

	private Map<String, ProviderInfo> updateProviderDetails(String authUserToken) throws Exception {
		Map<String, ProviderInfo> providerMap = null;

		// Get facets from Composite Search
		Map<String, String> headers = new HashMap<>();
		headers.put(Constants.USER_TOKEN, authUserToken);
		headers.put(Constants.AUTHORIZATION, cbExtServerProperties.getSbApiKey());

		HashMap<String, Object> reqBody = new HashMap<>();
		HashMap<String, Object> req = new HashMap<>();
		req.put(Constants.FACETS, Arrays.asList(Constants.SOURCE));
		Map<String, Object> filters = new HashMap<>();
		filters.put(Constants.PRIMARY_CATEGORY, Arrays.asList(Constants.COURSE, Constants.PROGRAM));
		filters.put(Constants.STATUS, Arrays.asList(Constants.LIVE));
		req.put(Constants.FILTERS, filters);
		req.put(Constants.LIMIT, 0);
		reqBody.put(Constants.REQUEST, req);

		Map<String, Object> compositeSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
				cbExtServerProperties.getKmBaseHost() + cbExtServerProperties.getKmCompositeSearchPath(), reqBody,
				headers);

		Map<String, Object> compositeSearchResult = (Map<String, Object>) compositeSearchRes.get(Constants.RESULT);
		List<Map<String, Object>> facetsList = (List<Map<String, Object>>) compositeSearchResult.get(Constants.FACETS);
		if (!CollectionUtils.isEmpty(facetsList)) {
			providerMap = new HashMap<String, ProviderInfo>();
			for (Map<String, Object> facetObj : facetsList) {
				String name = (String) facetObj.get(Constants.NAME);
				if (Constants.SOURCE.equalsIgnoreCase(name)) {
					List<Map<String, Object>> facetValueList = (List<Map<String, Object>>) facetObj
							.get(Constants.VALUES);
					if (!CollectionUtils.isEmpty(facetValueList)) {
						for (Map<String, Object> facetValueObj : facetValueList) {
							ProviderInfo provInfo = new ProviderInfo();
							provInfo.setName((String) facetValueObj.get(Constants.NAME));
							provInfo.setContentCount((int) facetValueObj.get(Constants.COUNT));
							providerMap.put((String) facetValueObj.get(Constants.NAME), provInfo);
						}
					}
				}
			}
		} else {
			Exception err = new Exception("Failed to get facets value from Composite Search API.");
			logger.error(err);
			try {
				logger.info("Received Response: " + (new ObjectMapper()).writeValueAsString(compositeSearchResult));
			} catch (Exception e) {
			}
			throw err;
		}

		// Get Provider Values
		reqBody = new HashMap<>();
		req = new HashMap<>();
		filters = new HashMap<>();
		filters.put(Constants.CHANNEL, providerMap.keySet().toArray());
		filters.put(Constants.IS_TENANT, true);
		req.put(Constants.FILTERS, filters);
		reqBody.put(Constants.REQUEST, req);

		Map<String, Object> orgSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
				cbExtServerProperties.getSbUrl() + cbExtServerProperties.getSbOrgSearchPath(), reqBody, headers);

		Map<String, Object> orgSearchResponse = (Map<String, Object>) ((Map<String, Object>) orgSearchRes
				.get(Constants.RESULT)).get(Constants.RESPONSE);

		List<Map<String, Object>> orgResponseList = (List<Map<String, Object>>) orgSearchResponse
				.get(Constants.CONTENT);

		if (!CollectionUtils.isEmpty(orgResponseList)) {
			for (Map<String, Object> respObj : orgResponseList) {
				String channelName = ((String) respObj.get(Constants.CHANNEL)).toLowerCase();
				if (providerMap.containsKey(channelName)) {
					ProviderInfo provInfo = providerMap.get(channelName);
					provInfo.setName((String) respObj.get(Constants.CHANNEL));
					provInfo.setDescription((String) respObj.get(Constants.DESCRIPTION));
					provInfo.setLogoUrl((String) respObj.get(Constants.IMG_URL_KEY));
					provInfo.setOrgId((String) respObj.get(Constants.ID));
					providerMap.put(channelName, provInfo);
				}
			}
		} else {
			Exception err = new Exception("Failed to get competency info from FRAC API.");
			logger.error(err);
			try {
				logger.info("Received Response: " + (new ObjectMapper()).writeValueAsString(orgSearchRes));
			} catch (Exception e) {
			}
			throw err;
		}

		redisCacheMgr.putCache(Constants.PROVIDER_CACHE_NAME, providerMap);
		return providerMap;
	}

	private Map<String, List<FracCommonInfo>> updateDesignationDetails(String authUserToken) throws Exception {
		Map<String, String> headers = new HashMap<>();
		HashMap<String, Object> reqBody = new HashMap<>();
		headers = new HashMap<>();
		headers.put(Constants.AUTHORIZATION, Constants.BEARER + authUserToken);
		reqBody = new HashMap<>();
		List<Map<String, Object>> searchList = new ArrayList<>();
		Map<String, Object> compSearchObj = new HashMap<>();
		compSearchObj.put(Constants.TYPE, Constants.POSITION.toUpperCase());
		compSearchObj.put(Constants.FIELD, Constants.NAME);
		compSearchObj.put(Constants.KEYWORD, StringUtils.EMPTY);
		searchList.add(compSearchObj);

		compSearchObj = new HashMap<String, Object>();
		compSearchObj.put(Constants.TYPE, Constants.POSITION.toUpperCase());
		compSearchObj.put(Constants.KEYWORD, Constants.VERIFIED);
		compSearchObj.put(Constants.FIELD, Constants.STATUS);
		searchList.add(compSearchObj);

		reqBody.put(Constants.SEARCHES, searchList);

		List<String> positionNameList = new ArrayList<String>();
		List<FracCommonInfo> positionList = getMasterPositionList(positionNameList);

		Map<String, Object> fracSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
				cbExtServerProperties.getFracHost() + cbExtServerProperties.getFracSearchPath(), reqBody, headers);
		List<Map<String, Object>> fracResponseList = (List<Map<String, Object>>) fracSearchRes
				.get(Constants.RESPONSE_DATA);
		if (!CollectionUtils.isEmpty(fracResponseList)) {
			for (Map<String, Object> respObj : fracResponseList) {
				if (!positionNameList.contains((String) respObj.get(Constants.NAME))) {
					positionList.add(new FracCommonInfo((String) respObj.get(Constants.ID),
							(String) respObj.get(Constants.NAME), (String) respObj.get(Constants.DESCRIPTION)));
					positionNameList.add((String) respObj.get(Constants.NAME));
				}
			}
		} else {
			Exception err = new Exception("Failed to get position info from FRAC API.");
			logger.error(err);
			try {
				logger.info("Received Response: " + (new ObjectMapper()).writeValueAsString(fracSearchRes));
			} catch (Exception e) {
			}
			throw err;
		}
		Map<String, List<FracCommonInfo>> positionMap = new HashMap<String, List<FracCommonInfo>>();
		positionMap.put(Constants.POSITIONS_CACHE_NAME, positionList);
		redisCacheMgr.putCache(Constants.POSITIONS_CACHE_NAME, positionMap);
		return positionMap;
	}

	private List<FracCommonInfo> getMasterPositionList(List<String> positionNameList) throws Exception {
		List<FracCommonInfo> positionList = new ArrayList<FracCommonInfo>();
		JsonNode jsonTree = new ObjectMapper().readTree(this.getClass().getClassLoader()
				.getResourceAsStream(cbExtServerProperties.getMasterPositionListFileName()));
		JsonNode positionsObj = jsonTree.get(Constants.POSITIONS);

		Iterator<JsonNode> positionsItr = positionsObj.elements();
		while (positionsItr.hasNext()) {
			JsonNode position = positionsItr.next();
			positionNameList.add(position.get(Constants.NAME).asText());
			positionList.add(new FracCommonInfo(position.get(Constants.ID).asText(),
					position.get(Constants.NAME).asText(), position.get(Constants.DESCRIPTION).asText()));
		}
		return positionList;
	}

	public SBApiResponse listCompetenciesByOrg(String orgId, String userToken) {
		SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.ORG_COMPETENCY_SEARCH_API);
		try {
			String userId = validateAuthTokenAndFetchUserId(userToken);
			if (org.apache.commons.lang3.StringUtils.isBlank(userId)) {
				response.getParams().setStatus(Constants.FAILED);
				response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
				response.setResponseCode(HttpStatus.BAD_REQUEST);
				return response;
			}
			List<String> courseIdList = redisCacheMgr.hget(Constants.COMPETENCIES_ORG_COURSES_REDIS_KEY, cbExtServerProperties.getRedisInsightIndex(), orgId);
			if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(courseIdList)) {
				Map<String, Object> competencyMap = listCompetencyDetails(Arrays.asList(courseIdList.get(0).split(",")));
				logger.info("Initializing/Refreshing the Cache Value for Key : " + Constants.COMPETENCY_CACHE_NAME);
				if (MapUtils.isNotEmpty(competencyMap)) {
					response.setResult((Map<String, Object>) competencyMap.get(Constants.RESULT));
				} else {
					response.getParams().setStatus(Constants.FAILED);
					response.getParams().setErrmsg("Competency map is empty");
					response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
				}
			} else {
				response.getParams().setStatus(Constants.FAILED);
				response.getParams().setErrmsg("Issue while fetching the competency for org: " + orgId);
				response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (Exception e) {
			logger.error("Issue while fetching the competency for org: " + orgId, e);
			response.getParams().setStatus(Constants.FAILED);
			response.getParams().setErrmsg(e.getMessage());
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return response;
	}

	private Map<String, Object> listCompetencyDetails(List<String> identifiers) throws Exception {

		HashMap<String, Object> reqBody = new HashMap<>();
		HashMap<String, Object> req = new HashMap<>();
		String competencySelected = cbExtServerProperties.getCompetencySelectedVersion();
		String facetsDetails = cbExtServerProperties.getCompetencySelectedVersionFacetsMap().get(competencySelected);
		req.put(Constants.FACETS, Arrays.asList(facetsDetails.split(",", -1)));
		Map<String, Object> filters = new HashMap<>();
		filters.put(Constants.IDENTIFIER, identifiers);
		filters.put(Constants.STATUS, Arrays.asList(Constants.LIVE));
		req.put(Constants.FILTERS, filters);
		req.put(Constants.LIMIT, 0);
		reqBody.put(Constants.REQUEST, req);

		Map<String, Object> compositeSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
				cbExtServerProperties.getSbSearchServiceHost() + cbExtServerProperties.getSbCompositeV4Search(), reqBody,
				null);

		return compositeSearchRes;
	}

	private String validateAuthTokenAndFetchUserId(String authUserToken) {
		return accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
	}
	public SBApiResponse getCompetencyDetailsV2() {
		SBApiResponse apiResponse = ProjectUtil.createDefaultResponse(Constants.COMPETENCY_DETAILS_API_V6);
		Map<String, Object> result = new HashMap<>();

		try {
			Map<String, Object> reqBody = prepareCompositeSearchRequestBody();
			Map<String, Object> compositeSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
					cbExtServerProperties.getSbSearchServiceHost() + cbExtServerProperties.getSbCompositeV4Search(), reqBody, null);

			if (compositeSearchRes == null || !compositeSearchRes.containsKey(Constants.RESULT)) {
				throw new Exception("Composite search result is null or missing.");
			}

			Map<String, Object> compositeSearchResult = (Map<String, Object>) compositeSearchRes.get(Constants.RESULT);
			List<Map<String, Object>> facetsList = (List<Map<String, Object>>) compositeSearchResult.get(Constants.FACETS);

			logger.info("facetsList :: "+facetsList);
			String url = cbExtServerProperties.getKmBaseHost() + cbExtServerProperties.getFrameworkReadEndpoint() + cbExtServerProperties.getKcmFrameworkName();
			logger.info("framework url:: "+url);

			Map<String, Object> fracSearchRes = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);

			if (fracSearchRes == null || !fracSearchRes.containsKey(Constants.RESULT)) {
				throw new Exception("Framework read result is null or missing.");
			}

			Map<String, Object> fracResponseList = (Map<String, Object>) fracSearchRes.get(Constants.RESULT);
			Map<String, Object> frameworkMap = (Map<String, Object>) fracResponseList.get(Constants.FRAMEWORK);
			List<Map<String, Object>> categories = (List<Map<String, Object>>) frameworkMap.get(Constants.CATEGORIES);

			// Validate and process the response
			if (!CollectionUtils.isEmpty(facetsList) && !CollectionUtils.isEmpty(categories)) {
				List<Map<String, Object>> contentList = processFacetData(facetsList, categories);
				result.put(Constants.CONTENT, contentList);
				apiResponse.setResult(result);
				apiResponse.setResponseCode(HttpStatus.OK);
			} else {
				throw new Exception("Facets or Framework categories are empty.");
			}

		} catch (Exception ex) {
			logger.error("Error while fetching competency details: ",ex);
			apiResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
			apiResponse.getParams().setErr("Failed to get competency details.");
			apiResponse.getParams().setErrmsg(ex.getMessage());
		}

		return apiResponse;
	}

	private Map<String, Object> prepareCompositeSearchRequestBody() {
		Map<String, Object> reqBody = new HashMap<>();
		Map<String, Object> req = new HashMap<>();
		req.put(Constants.FACETS, Arrays.asList(
				Constants.COMPETENCIES_V6_AREA_NAME,
				Constants.COMPETENCIES_V6_THEME_NAME,
				Constants.COMPETENCIES_V6_SUB_THEME_NAME
		));
		Map<String, Object> filters = new HashMap<>();
		filters.put(Constants.COURSE_CATEGORY, cbExtServerProperties.getCompetencyV6SearchPrimaryCategoryFilter());
		filters.put(Constants.STATUS, Arrays.asList(Constants.LIVE));
		req.put(Constants.FILTERS, filters);
		req.put(Constants.LIMIT, 0);
		reqBody.put(Constants.REQUEST, req);

		return reqBody;
	}

	private List<Map<String, Object>> processFacetData(List<Map<String, Object>> facetsList, List<Map<String, Object>> categories) throws Exception {
		List<Map<String, Object>> contentList = new ArrayList<>();

		Map<String, Map<String, Object>> competencyAreaTerms = extractFrameworkTerms(categories, Constants.COMPETENCYAREA);
		Map<String, Map<String, Object>> competencyThemeTerms = extractFrameworkTerms(categories, Constants.THEME);
		Map<String, Map<String, Object>> competencySubThemeTerms = extractFrameworkTerms(categories, Constants.SUB_THEME);


		for (Map<String, Object> facet : facetsList) {
			String facetName = (String) facet.get(Constants.NAME);
			List<Map<String, Object>> facetValues = (List<Map<String, Object>>) facet.get(Constants.VALUES);

			if (Constants.COMPETENCIES_V6_AREA_NAME.equalsIgnoreCase(facetName)) {
				for (Map<String, Object> area : facetValues) {
					String areaName = ((String) area.get(Constants.NAME)).toLowerCase();
					int areaCount = (int) area.get(Constants.COUNT);

					if (competencyAreaTerms.containsKey(areaName)) {
						Map<String, Object> areaMap = buildAreaMap(competencyAreaTerms, competencyThemeTerms, competencySubThemeTerms, facetsList, areaName, areaCount);
						contentList.add(areaMap);
					}
				}
			}
		}

		return contentList;
	}

	private Map<String, Object> buildAreaMap(Map<String, Map<String, Object>> competencyAreaTerms,
											 Map<String, Map<String, Object>> competencyThemeTerms,
											 Map<String, Map<String, Object>> competencySubThemeTerms,
											 List<Map<String, Object>> facetsList,
											 String areaName, int areaCount) throws Exception {

		Map<String, Object> areaTerm = competencyAreaTerms.get(areaName);
		List<Map<String, Object>> themes = (List<Map<String, Object>>) areaTerm.get(Constants.ASSOCIATIONS);

		Map<String, Object> areaMap = new HashMap<>();
		areaMap.put(Constants.NAME, areaName);
		areaMap.put(Constants.COUNT, areaCount);
		areaMap.put(Constants.DESCRIPTION, areaTerm.get(Constants.DESCRIPTION));
		areaMap.put(Constants.REFID, areaTerm.get(Constants.REFID));
		areaMap.put(Constants.IDENTIFIER, areaTerm.get(Constants.IDENTIFIER));
		Map<String, String> additionalProperties = (areaTerm.get(Constants.ADDITIONAL_PROPERTIES) instanceof Map)
				? (Map<String, String>) areaTerm.get(Constants.ADDITIONAL_PROPERTIES)
				: new HashMap<>();
		areaMap.put(Constants.DISPLAY_NAME, additionalProperties.getOrDefault(Constants.DISPLAY_NAME, ""));
		List<Map<String, Object>> themeList = new ArrayList<>();
		if (themes != null) {
			for (Map<String, Object> theme : themes) {
				String themeName = ((String) theme.get(Constants.NAME)).toLowerCase();
				if (competencyThemeTerms.containsKey(themeName)) {
					Map<String, Object> themeMap = buildThemeMap(competencyThemeTerms, competencySubThemeTerms, facetsList, themeName);
					themeList.add(themeMap);
				}
			}
		}

		areaMap.put(Constants.CHILDREN, themeList);
		return areaMap;
	}

	private Map<String, Object> buildThemeMap(Map<String, Map<String, Object>> competencyThemeTerms,
											  Map<String, Map<String, Object>> competencySubThemeTerms,
											  List<Map<String, Object>> facetsList, String themeName) throws Exception {
		Map<String, Object> themeTerm = competencyThemeTerms.get(themeName);

		Map<String, Object> themeFacet = findFacetByName(facetsList, Constants.COMPETENCIES_V6_THEME_NAME, themeName);
		int themeCount = themeFacet != null ? (int) themeFacet.get(Constants.COUNT) : 0;

		List<Map<String, Object>> subThemes = (List<Map<String, Object>>) themeTerm.get(Constants.ASSOCIATIONS);

		Map<String, Object> themeMap = new HashMap<>();
		themeMap.put(Constants.NAME, themeName);

		themeMap.put(Constants.COUNT, themeCount);
		themeMap.put(Constants.DESCRIPTION, themeTerm.get(Constants.DESCRIPTION));
		themeMap.put(Constants.REFID, themeTerm.get(Constants.REFID));
		themeMap.put(Constants.IDENTIFIER, themeTerm.get(Constants.IDENTIFIER));
		Map<String, String> additionalProperties = (themeTerm.get(Constants.ADDITIONAL_PROPERTIES) instanceof Map)
				? (Map<String, String>) themeTerm.get(Constants.ADDITIONAL_PROPERTIES)
				: new HashMap<>();
		themeMap.put(Constants.DISPLAY_NAME,additionalProperties.getOrDefault(Constants.DISPLAY_NAME, ""));

		List<Map<String, Object>> subThemeList = new ArrayList<>();
		if (subThemes != null) {
			for (Map<String, Object> subTheme : subThemes) {
				String subThemeName = ((String) subTheme.get(Constants.NAME)).toLowerCase();
				if (competencySubThemeTerms.containsKey(subThemeName)) {
					Map<String, Object> subThemeMap = buildSubThemeMap(competencySubThemeTerms, facetsList, subThemeName);
					subThemeList.add(subThemeMap);
				}
			}
		}

		themeMap.put(Constants.CHILDREN, subThemeList);
		return themeMap;
	}

	private Map<String, Object> buildSubThemeMap(Map<String, Map<String, Object>> competencySubThemeTerms,
												 List<Map<String, Object>> facetsList, String subThemeName) throws Exception {
		Map<String, Object> subThemeTerm = competencySubThemeTerms.get(subThemeName);

		Map<String, Object> subThemeFacet = findFacetByName(facetsList, Constants.COMPETENCIES_V6_SUB_THEME_NAME, subThemeName);
		int subThemeCount = subThemeFacet != null ? (int) subThemeFacet.get(Constants.COUNT) : 0;

		Map<String, Object> subThemeMap = new HashMap<>();
		subThemeMap.put(Constants.NAME, subThemeName);


		subThemeMap.put(Constants.COUNT, subThemeCount);

		subThemeMap.put(Constants.DESCRIPTION, subThemeTerm.get(Constants.DESCRIPTION));
		subThemeMap.put(Constants.REFID, subThemeTerm.get(Constants.REFID));
		subThemeMap.put(Constants.IDENTIFIER, subThemeTerm.get(Constants.IDENTIFIER));
		Map<String, String> additionalProperties = (subThemeTerm.get(Constants.ADDITIONAL_PROPERTIES) instanceof Map)
				? (Map<String, String>) subThemeTerm.get(Constants.ADDITIONAL_PROPERTIES)
				: new HashMap<>();
		subThemeMap.put(Constants.DISPLAY_NAME,additionalProperties.getOrDefault(Constants.DISPLAY_NAME, ""));

		return subThemeMap;
	}

	private Map<String, Object> findFacetByName(List<Map<String, Object>> facetsList, String facetName, String termName) {
		for (Map<String, Object> facet : facetsList) {
			if (facetName.equalsIgnoreCase((String) facet.get(Constants.NAME))) {
				List<Map<String, Object>> facetValues = (List<Map<String, Object>>) facet.get(Constants.VALUES);
				for (Map<String, Object> value : facetValues) {
					if (termName.equalsIgnoreCase((String) value.get(Constants.NAME))) {
						return value;
					}
				}
			}
		}
		return null;
	}

	private Map<String, Map<String, Object>> extractFrameworkTerms(List<Map<String, Object>> categories, String name) {
		Map<String, Map<String, Object>> termsMap = new HashMap<>();
		for (Map<String, Object> category : categories) {
			if (name.equalsIgnoreCase((String) category.get(Constants.NAME))) {
				List<Map<String, Object>> terms = (List<Map<String, Object>>) category.get(Constants.TERMS);
				for (Map<String, Object> term : terms) {
					termsMap.put(((String) term.get(Constants.NAME)).toLowerCase(), term);
				}
				break;
			}
		}
		return termsMap;
	}

}
