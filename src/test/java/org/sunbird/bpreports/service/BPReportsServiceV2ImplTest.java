package org.sunbird.bpreports.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sunbird.bpreports.postgres.repository.WfStatusEntityRepository;
import org.sunbird.cassandra.utils.CassandraOperationImpl;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.IndexerService;
import org.sunbird.core.producer.Producer;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorEntity;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorRoleEntity;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRepository;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRoleRepository;
import org.sunbird.storage.service.StorageService;
import org.sunbird.user.service.UserUtilityService;

import com.fasterxml.jackson.databind.ObjectMapper;

class BPReportsServiceV2ImplTest {

    private static final String PROGRAM_ID = "do_12345";
    private static final Short MAIN_PC_ROLE_ID = 1;
    private static final Short SMT_ROLE_ID = 30;
    private static final Short SLT_ROLE_ID = 20;

    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private UserUtilityService userUtilityService;
    @Mock
    private CassandraOperationImpl cassandraOperation;
    @Mock
    private Producer kafkaProducer;
    @Mock
    private CbExtServerProperties serverProperties;
    @Mock
    private WfStatusEntityRepository wfStatusEntityRepository;
    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    private StorageService storageService;
    @Mock
    private IndexerService indexerService;
    @Mock
    private ProgramCoordinatorRepository programCoordinatorRepository;
    @Mock
    private ProgramCoordinatorRoleRepository programCoordinatorRoleRepository;

    private ObjectMapper objectMapper;
    private BPReportsServiceV2Impl service;
    private XSSFWorkbook workbook;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        objectMapper = new ObjectMapper();
        service = new BPReportsServiceV2Impl(accessTokenValidator, userUtilityService, cassandraOperation,
                kafkaProducer, serverProperties, wfStatusEntityRepository, outboundRequestHandlerService,
                objectMapper, storageService, indexerService, programCoordinatorRepository,
                programCoordinatorRoleRepository);
        workbook = new XSSFWorkbook();
        when(serverProperties.getBpReportCassandraInChunkSize()).thenReturn(100);
    }

    @AfterEach
    void tearDown() throws Exception {
        workbook.close();
    }

    private ProgramCoordinatorRoleEntity role(Short id, String code, String name) {
        return ProgramCoordinatorRoleEntity.builder().id(id).roleCode(code).roleName(name).build();
    }

    private ProgramCoordinatorEntity coordinator(UUID userId, Short roleId, UUID createdBy) {
        ProgramCoordinatorEntity entity = new ProgramCoordinatorEntity();
        entity.setProgramId(PROGRAM_ID);
        entity.setUserId(userId);
        entity.setRoleId(roleId);
        entity.setStatus((short) 1);
        entity.setCreatedBy(createdBy);
        entity.setCreatedOn(new java.sql.Timestamp(System.currentTimeMillis()));
        return entity;
    }

    private Map<String, Object> cassandraUserRow(String firstName, String email, String designation)
            throws Exception {
        Map<String, Object> personalDetails = new HashMap<>();
        personalDetails.put(Constants.PRIMARY_EMAIL, email);
        Map<String, Object> professionalDetail = new HashMap<>();
        professionalDetail.put(Constants.DESIGNATION, designation);
        professionalDetail.put(Constants.GROUP, "SomeGroup");
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.PERSONAL_DETAILS, personalDetails);
        profileDetails.put(Constants.PROFESSIONAL_DETAILS, Collections.singletonList(professionalDetail));

        Map<String, Object> row = new HashMap<>();
        row.put(Constants.FIRSTNAME, firstName);
        row.put(Constants.PROFILE_DETAILS_LOWER, objectMapper.writeValueAsString(profileDetails));
        return row;
    }

    @Test
    void shouldIncludeOnlyTrainersAddedByMainPc() throws Exception {
        UUID mainPcId = UUID.randomUUID();
        UUID smtAddedByMainPc = UUID.randomUUID();
        UUID sltAddedBySomeoneElse = UUID.randomUUID();
        UUID otherTrainerId = UUID.randomUUID();

        List<ProgramCoordinatorEntity> coordinators = Arrays.asList(
                coordinator(mainPcId, MAIN_PC_ROLE_ID, mainPcId),
                coordinator(smtAddedByMainPc, SMT_ROLE_ID, mainPcId),
                coordinator(sltAddedBySomeoneElse, SLT_ROLE_ID, otherTrainerId));
        when(programCoordinatorRepository.findActiveByProgramId(PROGRAM_ID)).thenReturn(coordinators);

        List<ProgramCoordinatorRoleEntity> roles = Arrays.asList(
                role(MAIN_PC_ROLE_ID, "PROGRAM_COORDINATOR", Constants.PROGRAM_COORDINATOR_KEY),
                role(SMT_ROLE_ID, "STATE_MASTER_TRAINER", "State Master Trainer"),
                role(SLT_ROLE_ID, "STATE_LEAD_TRAINER", "State Lead Trainer"));
        when(programCoordinatorRoleRepository.findAll()).thenReturn(roles);

        Map<String, Object> cassandraResult = new HashMap<>();
        cassandraResult.put(smtAddedByMainPc.toString(),
                cassandraUserRow("Trainer One", "trainer1@example.com", "District Officer"));
        cassandraResult.put(mainPcId.toString(),
                cassandraUserRow("Main Pc", "mainpc@example.com", "Program Lead"));
        when(cassandraOperation.getRecordsByProperties(eq(Constants.SUNBIRD_KEY_SPACE_NAME), eq(Constants.TABLE_USER),
                anyMap(), any(), eq(Constants.ID))).thenReturn(cassandraResult);

        service.buildProgramCoordinatorSheet(workbook, PROGRAM_ID);

        Sheet sheet = workbook.getSheet("Program Coordinators");
        assertEquals(2, sheet.getPhysicalNumberOfRows());

        Row headerRow = sheet.getRow(0);
        assertEquals("Name", headerRow.getCell(0).getStringCellValue());
        assertEquals("Added-by", headerRow.getCell(5).getStringCellValue());

        Row dataRow = sheet.getRow(1);
        assertEquals("Trainer One", dataRow.getCell(0).getStringCellValue());
        assertEquals("trainer1@example.com", dataRow.getCell(1).getStringCellValue());
        assertEquals("State Master Trainer", dataRow.getCell(2).getStringCellValue());
        assertEquals("Main Pc", dataRow.getCell(5).getStringCellValue());
    }

    @Test
    void shouldSkipSheetWhenNoCoordinatorsFound() {
        when(programCoordinatorRepository.findActiveByProgramId(PROGRAM_ID)).thenReturn(Collections.emptyList());

        service.buildProgramCoordinatorSheet(workbook, PROGRAM_ID);

        assertNull(workbook.getSheet("Program Coordinators"));
    }

    @Test
    void shouldSkipSheetWhenMainPcRoleIsNotConfigured() {
        when(programCoordinatorRepository.findActiveByProgramId(PROGRAM_ID))
                .thenReturn(Collections.singletonList(coordinator(UUID.randomUUID(), SMT_ROLE_ID, UUID.randomUUID())));
        when(programCoordinatorRoleRepository.findAll())
                .thenReturn(Collections.singletonList(role(SMT_ROLE_ID, "STATE_MASTER_TRAINER", "State Master Trainer")));

        service.buildProgramCoordinatorSheet(workbook, PROGRAM_ID);

        assertNull(workbook.getSheet("Program Coordinators"));
    }

    @Test
    void shouldSkipSheetWhenNoTrainerWasAddedByMainPc() {
        UUID mainPcId = UUID.randomUUID();
        UUID otherActor = UUID.randomUUID();
        UUID trainerId = UUID.randomUUID();

        when(programCoordinatorRepository.findActiveByProgramId(PROGRAM_ID)).thenReturn(Arrays.asList(
                coordinator(mainPcId, MAIN_PC_ROLE_ID, mainPcId),
                coordinator(trainerId, SMT_ROLE_ID, otherActor)));
        when(programCoordinatorRoleRepository.findAll()).thenReturn(Arrays.asList(
                role(MAIN_PC_ROLE_ID, "PROGRAM_COORDINATOR", Constants.PROGRAM_COORDINATOR_KEY),
                role(SMT_ROLE_ID, "STATE_MASTER_TRAINER", "State Master Trainer")));

        service.buildProgramCoordinatorSheet(workbook, PROGRAM_ID);

        assertNull(workbook.getSheet("Program Coordinators"));
    }

    @Test
    void shouldSwallowExceptionFromRepository() {
        when(programCoordinatorRepository.findActiveByProgramId(PROGRAM_ID))
                .thenThrow(new RuntimeException("db down"));

        service.buildProgramCoordinatorSheet(workbook, PROGRAM_ID);

        assertNull(workbook.getSheet("Program Coordinators"));
    }
}
