package org.sunbird.programcoordinator.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorListItem;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRepository;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRoleRepository;

@Service
public class ProgramCoordinatorServiceImpl implements ProgramCoordinatorService {

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    // API-facing sort keys (camelCase, matching the list response's field names) mapped to the
    // actual native-query column they translate to. Anything not in this map is treated as "no sort".
    private static final Map<String, String> SORTABLE_FIELDS = new HashMap<>();
    static {
        SORTABLE_FIELDS.put("roleId", "role_id");
        SORTABLE_FIELDS.put("userId", "user_id");
    }

    @Autowired
    private ProgramCoordinatorRepository programCoordinatorRepository;

    @Autowired
    private ProgramCoordinatorRoleRepository programCoordinatorRoleRepository;

    @Override
    public SBApiResponse upsert(String programId, ProgramCoordinatorUpsertRequest request, String actorId)
            throws Exception {
        SBApiResponse response = new SBApiResponse(Constants.API_PROGRAM_COORDINATOR_UPSERT);
        try {
            if (StringUtils.isEmpty(programId) || request.getUserId() == null || request.getStatus() == null
                    || (request.getStatus() != 1 && request.getStatus() != 0)) {
                response.getParams().setErrmsg("programId, userId and status (0 or 1) are required");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            UUID actorUuid = UUID.fromString(actorId);

            if (request.getStatus() == 1) {
                if (request.getRoleId() == null) {
                    response.getParams().setErrmsg("roleId is required to add a coordinator");
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                if (!programCoordinatorRoleRepository.existsById(request.getRoleId())) {
                    response.getParams().setErrmsg("Invalid roleId: " + request.getRoleId());
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }

                int rows = programCoordinatorRepository.addOrResurrect(programId, request.getUserId(),
                        request.getRoleId(), actorUuid);
                if (rows == 0) {
                    String errMsg = "User " + request.getUserId() + " is already an active coordinator on programme "
                            + programId;
                    logger.info(errMsg);
                    response.getParams().setErrmsg(errMsg);
                    response.setResponseCode(HttpStatus.CONFLICT);
                    return response;
                }
            } else {
                int rows = programCoordinatorRepository.softRemove(programId, request.getUserId(), actorUuid);
                if (rows == 0) {
                    String errMsg = "User " + request.getUserId() + " is not an active coordinator on programme "
                            + programId;
                    logger.info(errMsg);
                    response.getParams().setErrmsg(errMsg);
                    response.setResponseCode(HttpStatus.NOT_FOUND);
                    return response;
                }
            }

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception ex) {
            String errMsg = "Exception occurred while upserting program coordinator. Exception: " + ex.getMessage();
            logger.error(errMsg, ex);
            response.getParams().setErrmsg(errMsg);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse list(String programId, int limit, int offset, String sortBy, String sortDirection)
            throws Exception {
        SBApiResponse response = new SBApiResponse(Constants.API_PROGRAM_COORDINATOR_LIST);
        try {
            if (StringUtils.isEmpty(programId)) {
                response.getParams().setErrmsg("programId is required");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            int safeLimit = limit > 0 ? limit : 20;
            int safeOffset = offset >= 0 ? offset : 0;
            int page = safeOffset / safeLimit;
            String sortColumn = SORTABLE_FIELDS.get(sortBy);

            Pageable pageable;
            if (sortColumn != null) {
                Sort.Direction direction = "desc".equalsIgnoreCase(sortDirection) ? Sort.Direction.DESC
                        : Sort.Direction.ASC;
                pageable = PageRequest.of(page, safeLimit, Sort.by(direction, sortColumn));
            } else {
                // No recognised sortBy -> no Sort at all, so no ORDER BY is added to the query.
                pageable = PageRequest.of(page, safeLimit);
            }

            Page<ProgramCoordinatorListItem> result = programCoordinatorRepository.findCoordinators(programId,
                    pageable);

            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put(Constants.CONTENT, result.getContent());
            responseMap.put(Constants.COUNT, result.getTotalElements());
            responseMap.put(Constants.LIMIT, safeLimit);
            responseMap.put(Constants.OFFSET, safeOffset);
            response.put(Constants.RESPONSE, responseMap);

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception ex) {
            String errMsg = "Exception occurred while listing program coordinators. Exception: " + ex.getMessage();
            logger.error(errMsg, ex);
            response.getParams().setErrmsg(errMsg);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }
}
