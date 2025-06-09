package org.sunbird.org.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.xssf.usermodel.XSSFDataValidation;
import org.apache.poi.xssf.usermodel.XSSFDataValidationHelper;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.org.util.ExcelUtil;
import org.sunbird.org.util.FrameworkUtil;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class OrgHierarchyServiceImpl implements OrgHierarchyService {

    private final Logger logger = LoggerFactory.getLogger(OrgHierarchyServiceImpl.class);


    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandler;

    @Autowired
    private CbExtServerProperties serverProperties;

    @Autowired
    RedisCacheMgr redisCacheMgr;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    ExcelUtil excelUtil;

    @Autowired
    FrameworkUtil frameworkUtil;


    @Override
    public ResponseEntity<ByteArrayResource> bulkUploadOrganisationMapping(String rootOrgId, String userAuthToken, String frameworkId) {
        try {
            Workbook workbook = new XSSFWorkbook();

            Sheet referenceSheetCompetency = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyReferenceWorkSpaceName()));
            Sheet orgDesignationMasterSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyMasterDesignationWorkSpaceName()));
            List<Map<String, Object>> categoryList = frameworkUtil.populateDataFromFrameworkTerm(frameworkId);
            String[] referenceSheetHeaders = categoryList.stream()
                    .map(cat -> (String) cat.get(Constants.CODE))
                    .filter(code -> code != null)
                    .toArray(String[]::new);
            excelUtil.createHeaderRowForYourWorkBook(referenceSheetCompetency, referenceSheetHeaders);
            excelUtil.createHeaderRow(orgDesignationMasterSheet, serverProperties.getBulkUploadOrgHierarchyMasterDataHeaders());

            frameworkUtil.populateOrgDesignationMaster(orgDesignationMasterSheet);

            excelUtil.makeSheetReadOnly(orgDesignationMasterSheet);

            orgDesignationMasterSheet.setColumnHidden(2, true);

            excelUtil.setUpDropdowns(workbook, referenceSheetCompetency, orgDesignationMasterSheet, serverProperties.getBulkUploadOrgHierarchyReferencesHeaders());

            excelUtil.setColumnWidths(referenceSheetCompetency);

            excelUtil.setColumnWidths(orgDesignationMasterSheet);

            excelUtil.addDuplicateHighlighting(referenceSheetCompetency, 1, 1000, 0, serverProperties.getBulkUploadOrgHierarchyReferencesHeaders().length - 1);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);

            workbook.close();

            // Convert the output stream to a byte array and return as a downloadable file
            ByteArrayResource resource = new ByteArrayResource(outputStream.toByteArray());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + serverProperties.getOrgHierarchyBulkUploadFileName() + "\"");

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(resource.contentLength())
                    .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (Exception e) {
            logger.error("Error while generating bulk upload organisation mapping file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public ResponseEntity<ByteArrayResource> exportOrgHierarchyToExcel(String userAuthToken, String frameworkId) {
        try {
            Workbook workbook = new XSSFWorkbook();

            Sheet referenceSheetCompetency = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyReferenceWorkSpaceName()));
            List<Map<String, Object>> categoryList = frameworkUtil.populateDataFromFrameworkTerm(frameworkId);
            String[] referenceSheetHeaders = categoryList.stream()
                    .map(cat -> (String) cat.get(Constants.CODE))
                    .filter(code -> code != null)
                    .toArray(String[]::new);
            excelUtil.createHeaderRowForYourWorkBook(referenceSheetCompetency, referenceSheetHeaders);

            excelUtil.makeSheetReadOnly(referenceSheetCompetency);


            excelUtil.setColumnWidths(referenceSheetCompetency);

            excelUtil.populateReferenceSheetCompetency(categoryList, referenceSheetCompetency);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);

            workbook.close();

            // Convert the output stream to a byte array and return as a downloadable file
            ByteArrayResource resource = new ByteArrayResource(outputStream.toByteArray());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + serverProperties.getOrgHierarchyBulkUploadFileName() + "\"");

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(resource.contentLength())
                    .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (Exception e) {
            logger.error("Error while exporting organisation hierarchy to Excel", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}