package org.sunbird.common.helper.cassandra;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.time.AtomicTimestampGenerator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.sunbird.common.exceptions.ProjectCommonException;
import org.sunbird.common.exceptions.ResponseCode;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.PropertiesCache;
import org.sunbird.org.service.OrgDesignationBulkUploadConsumer;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class CassandraConnectionManagerImpl implements CassandraConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionManagerImpl.class);
    private static final Map<String, CqlSession> cassandraSessionMap = new ConcurrentHashMap<>(2);
    private static CqlSession session;

    @Autowired
    private OrgDesignationBulkUploadConsumer orgDesignationBulkUploadConsumer = null;

    @Override
    public CqlSession getSession(String keyspace) {
        CqlSession currentSession = cassandraSessionMap.get(keyspace);
        if (currentSession != null && !currentSession.isClosed()) {
            return currentSession;
        } else {
            logger.info("CassandraConnectionManagerImpl:: Creating connection for :: " + keyspace);
            CqlSession newSession = createCassandraConnectionWithKeySpaces(keyspace);
            cassandraSessionMap.put(keyspace, newSession);
            return newSession;
        }
    }

    public CassandraConnectionManagerImpl() {
        // Initialize the connection and register shutdown hook
        registerShutDownHook();
        createCassandraConnection();
    }

    private void createCassandraConnection() {
        try {
            session = createCassandraConnectionWithKeySpaces(null);
        } catch (Exception e) {
            logger.error("Error while creating Cassandra connection", e);
            throw new ProjectCommonException(
                    Constants.ERROR,
                    e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    private CqlSession createCassandraConnectionWithKeySpaces(String keySpaceName) {
        try {
            // Load the properties required for connection
            PropertiesCache cache = PropertiesCache.getInstance();
            String cassandraHost = cache.getProperty(Constants.CASSANDRA_CONFIG_HOST);
            if (StringUtils.isBlank(cassandraHost)) {
                throw new ProjectCommonException(
                        Constants.ERROR,
                        "Cassandra host is not configured",
                        HttpStatus.INTERNAL_SERVER_ERROR.value());
            }
            List<String> hosts = Arrays.asList(cassandraHost.split(","));
            List<InetSocketAddress> contactPoints = hosts.stream()
                    .map(host -> new InetSocketAddress(host.trim(), 9042)) // Assuming default port 9042
                    .collect(Collectors.toList());
            List<String> contactPointsString = hosts.stream()
                    .map(host -> host.trim() + ":9042") // Ensure proper host:port format
                    .collect(Collectors.toList());
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.CONTACT_POINTS, contactPointsString)
                    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, getConsistencyLevel().name())
                    .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE,
                            Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE,
                            Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)))
                    .withInt(DefaultDriverOption.HEARTBEAT_INTERVAL,
                            Integer.parseInt(cache.getProperty(Constants.HEARTBEAT_INTERVAL)))
                    .withInt(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, 10000)
                    .withInt(DefaultDriverOption.REQUEST_TIMEOUT, 10000)
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.toString())
                    .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy.class)
                    .withClass(DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS, AtomicTimestampGenerator.class)
                    .build();
            CqlSession sessionWithKeyspaces;
            if (StringUtils.isNotBlank(keySpaceName)) {
                sessionWithKeyspaces = CqlSession.builder()
                        .addContactPoints(contactPoints)
                        .withLocalDatacenter("datacenter1")
                        .withKeyspace(keySpaceName)
                        .withConfigLoader(loader)
                        .build();
            } else {
                sessionWithKeyspaces = CqlSession.builder()
                        .addContactPoints(contactPoints)
                        .withLocalDatacenter("datacenter1")
                        .withConfigLoader(loader)
                        .build();
            }
            logger.info("Connected to the keyspaces: " + keySpaceName);
            // Get metadata and log cluster information
            final Metadata metadata = sessionWithKeyspaces.getMetadata();
            logger.info(String.format("Connected to cluster: %s", metadata.getClusterName()));
            // Log nodes in the cluster
            for (Node host : metadata.getNodes().values()) {
                logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getEndPoint(), host.getRack()));
            }
            return sessionWithKeyspaces;
        } catch (Exception e) {
            logger.error("Error while creating Cassandra connection", e);
            throw new ProjectCommonException(
                    Constants.ERROR,
                    e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    private static ConsistencyLevel getConsistencyLevel() {
        String consistency = PropertiesCache.getInstance().readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL);
        logger.info("CassandraConnectionManagerImpl:getConsistencyLevel: level = " + consistency);
        if (StringUtils.isBlank(consistency)) {
            logger.warn("Consistency level not defined in config. Defaulting to LOCAL_QUORUM.");
            return DefaultConsistencyLevel.LOCAL_QUORUM;
        }

        try {
            return DefaultConsistencyLevel.valueOf(consistency.toUpperCase());
        } catch (IllegalArgumentException exception) {
            logger.info("CassandraConnectionManagerImpl:getConsistencyLevel: Exception occurred with error message = "
                    + exception.getMessage());
            return DefaultConsistencyLevel.LOCAL_QUORUM;
        }
    }

    /**
     * Register the hook for resource clean up. this will be called when jvm shut down.
     */
    public static void registerShutDownHook() {
        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new ResourceCleanUp());
        logger.info("Cassandra ShutDownHook registered.");
    }

    /**
     * This class will be called by registerShutDownHook to register the call inside jvm , when jvm
     * terminate it will call the run method to clean up the resource.
     */
    static class ResourceCleanUp extends Thread {
        @Override
        public void run() {
            try {
                logger.info("started resource cleanup Cassandra.");
                for (Map.Entry<String, CqlSession> entry : cassandraSessionMap.entrySet()) {
                    entry.getValue().close();
                }
                if (session != null) {
                    session.close();
                }
                logger.info("completed resource cleanup Cassandra.");
            } catch (Exception ex) {
                logger.error("Error during resource cleanup", ex);
            }
        }
    }

    public void registerShutDownHookV2() {
        Runtime runtime = Runtime.getRuntime();

        // Adding a shutdown hook that ensures OrgDesignationBulkUploadConsumer shutdown happens first
        runtime.addShutdownHook(new Thread(() -> {
            try {
                // First, explicitly call shutdown logic for OrgDesignationBulkUploadConsumer
                if (orgDesignationBulkUploadConsumer != null) {
                    logger.info("Processing buffered messages during shutdown...");
                    orgDesignationBulkUploadConsumer.shutdownHook();
                    logger.info("Buffered messages processed successfully.");
                } else {
                    logger.info("orgDesignationBulkUploadConsumer is not active. Skipping message processing.");
                }
            } catch (Exception e) {
                logger.error("Error occurred while processing buffered messages during shutdown.", e);
            }

            try {
                logger.info("Starting Cassandra cleanup...");
                new ResourceCleanUp().run();  // Assuming this handles Cassandra's cleanup logic
                logger.info("Cassandra ShutDownHook completed.");
            } catch (Exception e) {
                logger.error("Error occurred during Cassandra cleanup.", e);
            }
        }));
        logger.info("Cassandra ShutDownHook registered.");
    }

    @Override
    public List<String> getTableList(String keyspaceName) {
        try {
            // Fetch the metadata for the keyspace and list tables
            Metadata metadata = session.getMetadata();
            if (metadata.getKeyspace(keyspaceName).isPresent()) {
                // Convert the Map<CqlIdentifier, TableMetadata> to a List<String> with table names
                Map<CqlIdentifier, TableMetadata> tables = metadata.getKeyspace(keyspaceName).get().getTables();
                return tables.keySet().stream()
                        .map(CqlIdentifier::toString)
                        .collect(Collectors.toList());
            } else {
                throw new ProjectCommonException(
                        ResponseCode.internalError.getErrorCode(),
                        "Keyspace not found: " + keyspaceName,
                        ResponseCode.SERVER_ERROR.getResponseCode());
            }
        } catch (Exception e) {
            logger.error("Error fetching tables for keyspace: " + keyspaceName, e);
            throw new ProjectCommonException(
                    ResponseCode.internalError.getErrorCode(),
                    e.getMessage(),
                    ResponseCode.SERVER_ERROR.getResponseCode());
        }
    }
}