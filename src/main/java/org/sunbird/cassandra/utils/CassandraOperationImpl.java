package org.sunbird.cassandra.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sunbird.common.helper.cassandra.CassandraConnectionManager;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.select.Selector.column;

@Component
public class CassandraOperationImpl implements CassandraOperation {

	private Logger logger = LoggerFactory.getLogger(getClass().getName());

	@Autowired
  	CassandraConnectionManager connectionManager;

	@Override
	public SBApiResponse insertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
		SBApiResponse response = new SBApiResponse();
		try {
			String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
			CqlSession session = connectionManager.getSession(keyspaceName);
			PreparedStatement statement = session.prepare(query);
			BoundStatement boundStatement = statement.bind(request.values().toArray());
			session.execute(boundStatement);
			response.put(Constants.RESPONSE, Constants.SUCCESS);
		} catch (Exception e) {
			String errMsg = String.format("Exception occurred while inserting record to %s %s", tableName, e.getMessage());
			logger.error(errMsg);
			response.put(Constants.RESPONSE, Constants.FAILED);
			response.put(Constants.ERROR_MESSAGE, errMsg);
		}
		return response;
	}

	@Override
	public SBApiResponse insertBulkRecord(String keyspaceName, String tableName, List<Map<String, Object>> request) {
		SBApiResponse response = new SBApiResponse();
		try {
			CqlSession session = connectionManager.getSession(keyspaceName);
			BatchStatement batchStatement =
					BatchStatement.builder(DefaultBatchType.LOGGED)
							.build();
			for (Map<String, Object> requestMap : request) {
				String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, requestMap);
				PreparedStatement statement = session.prepare(query);
				BoundStatement boundStatement = statement.bind(requestMap.values().toArray());
				batchStatement = batchStatement.add(boundStatement);
			}
			session.execute(batchStatement);
			response.put(Constants.RESPONSE, Constants.SUCCESS);
		} catch (Exception e) {
			logger.error(String.format("Exception occurred while inserting bulk record to %s %s", tableName,
					e.getMessage()));
		}
		return response;
	}

	@Override
	public List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName,
			Map<String, Object> propertyMap, List<String> fields) {
		List<Map<String, Object>> response = new ArrayList<>();
		try {
			Select selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);
			SimpleStatement statement = selectQuery.build();
			CqlSession session = connectionManager.getSession(keyspaceName);
			ResultSet results = session.execute(statement);
			response = CassandraUtil.createResponse(results);

		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	@Override
	public Map<String, Object> getRecordsByProperties(String keyspaceName, String tableName,
			Map<String, Object> propertyMap, List<String> fields, String key) {
		Map<String, Object> response = new HashMap<>();
		try {
			Select selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);
			SimpleStatement statement = selectQuery.build();
			CqlSession session = connectionManager.getSession(keyspaceName);
			ResultSet results = session.execute(statement);
			response = CassandraUtil.createResponse(results, key);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	@Override
	public List<Map<String, Object>> searchByWhereClause(String keyspace, String tableName, List<String> fields,
			Date date) {
		try {
			Select selectQuery;
			if (CollectionUtils.isNotEmpty(fields)) {
				selectQuery = QueryBuilder.selectFrom(keyspace, tableName).columns(fields);
			} else {
				selectQuery = QueryBuilder.selectFrom(keyspace, tableName).all();
			}
			selectQuery = selectQuery.whereColumn("completionpercentage").isGreaterThan(literal(0))
					.whereColumn("completionpercentage").isLessThan(literal(100))
					.whereColumn("last_access_time").isGreaterThan(literal(0))
					.whereColumn("last_access_time").isLessThan(literal(date));
			SimpleStatement statement = SimpleStatement.builder(selectQuery.toString())
					.setExecutionProfileName("query-with-filtering")
					.build();
			logger.debug("our query: {}", statement.getQuery());
			ResultSet resultSet = connectionManager.getSession(keyspace).execute(statement);
			return CassandraUtil.createResponse(resultSet);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
			return new ArrayList<>();
		}
	}

	@Override
	public Map<String, Object> getRecordsByPropertiesWithPagination(String keyspaceName, String tableName,
			Map<String, Object> propertyMap, List<String> fields, int limit, String updatedOn, String key) {
		Map<String, Object> response = new HashMap<>();
		try {
			Select selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);
			selectQuery = selectQuery.limit(limit);
			if (StringUtils.isNotEmpty(updatedOn)) {
				selectQuery = selectQuery.whereColumn("updatedon").isLessThan(literal(UUID.fromString(updatedOn)));
			}
			SimpleStatement statement = selectQuery.build();
			CqlSession session = connectionManager.getSession(keyspaceName);
			ResultSet results = session.execute(statement);
			response = CassandraUtil.createResponse(results, key);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	private Select processQuery(String keyspaceName, String tableName, Map<String, Object> propertyMap,
								List<String> fields) {

		// Check if we have multiple IN clauses that might cause the error
		boolean hasMultipleListValues = propertyMap.values().stream()
				.filter(v -> v instanceof List && ((List<?>) v).size() > 1)
				.count() > 1;

		// If we have multiple IN clauses, we need to handle it differently
		if (hasMultipleListValues) {
			return processQueryForMultipleInClauses(keyspaceName, tableName, propertyMap, fields);
		}

		// Normal processing for simpler cases
		Select selectFrom;
		if (CollectionUtils.isNotEmpty(fields)) {
			selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields.toArray(new String[0]));
		} else {
			selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).all();
		}

		Select selectQuery = selectFrom;
		if (MapUtils.isNotEmpty(propertyMap)) {
			for (Entry<String, Object> entry : propertyMap.entrySet()) {
				if (entry.getValue() instanceof List) {
					List<?> list = (List<?>) entry.getValue();
					if (CollectionUtils.isNotEmpty(list)) {
						// If there's only one value in the list, use equals instead of IN
						if (list.size() == 1) {
							selectQuery = selectQuery.whereColumn(entry.getKey())
									.isEqualTo(QueryBuilder.literal(list.get(0)));
						} else {
							List<Term> terms = list.stream()
									.map(QueryBuilder::literal)
									.collect(Collectors.toList());
							selectQuery = selectQuery.whereColumn(entry.getKey()).in(terms);
						}
					}
				} else {
					selectQuery = selectQuery.whereColumn(entry.getKey())
							.isEqualTo(QueryBuilder.literal(entry.getValue()));
				}
			}
			//selectQuery = selectQuery.allowFiltering();
		}

		return selectQuery;
	}

	@Override
	public void deleteRecord(String keyspaceName, String tableName, Map<String, Object> compositeKeyMap) {
		try {
			DeleteSelection deleteSelection =
					QueryBuilder.deleteFrom(keyspaceName, tableName);
			Delete deleteQuery = null;
			for (Map.Entry<String, Object> entry : compositeKeyMap.entrySet()) {
				if (deleteQuery == null) {
					deleteQuery = deleteSelection.whereColumn(entry.getKey())
							.isEqualTo(literal(entry.getValue()));
				} else {
					deleteQuery = deleteQuery.whereColumn(entry.getKey())
							.isEqualTo(literal(entry.getValue()));
				}
			}
			CqlSession session = connectionManager.getSession(keyspaceName);
			session.execute(deleteQuery.build());
		} catch (Exception e) {
			logger.error("CassandraOperationImpl: deleteRecord by composite key. {} {} {}",
					Constants.EXCEPTION_MSG_DELETE, tableName, e.getMessage(), e);
			throw e;
		}
	}

	@Override
	public Map<String, Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
			Map<String, Object> compositeKey) {
		Map<String, Object> response = new HashMap<>();
		try {
			CqlSession session = connectionManager.getSession(keyspaceName);
			UpdateStart updateStart = QueryBuilder.update(keyspaceName, tableName);
			UpdateWithAssignments updateWithAssignments = updateStart.set(updateAttributes.entrySet().stream()
					.map(entry -> Assignment.setColumn(entry.getKey(), literal(entry.getValue())))
					.toArray(Assignment[]::new));
			Update update = updateWithAssignments.where(compositeKey.entrySet().stream()
					.map(entry -> Relation.column(entry.getKey()).isEqualTo(literal(entry.getValue())))
					.toArray(Relation[]::new));
			SimpleStatement statement = update.build();
			session.execute(statement);
			response.put(Constants.RESPONSE, Constants.SUCCESS);
		} catch (Exception e) {
			String errMsg = String.format("Exception occurred while updating record to %s %s", tableName, e.getMessage());
			logger.error(errMsg);
			response.put(Constants.RESPONSE, Constants.FAILED);
			response.put(Constants.ERROR_MESSAGE, errMsg);
			throw e;
		}
		return response;
	}

	@Override
	public Long getRecordCount(String keyspace, String table) {
		try {
			Select selectQuery = QueryBuilder.selectFrom(keyspace, table).countAll();
			CqlSession session = connectionManager.getSession(keyspace);
			Row row = session.execute(selectQuery.build()).one();
			return row.getLong(0);
		} catch (Exception e) {
			logger.error("Error getting record count for table {}: {}", table, e.getMessage(), e);
			throw e;
		}
	}

	public void getAllRecords(String keyspace, String table, List<String> fields, String key,
			Map<String, Map<String, String>> objectInfo) {
		Select selectQuery = null;
		try {
			selectQuery = processQuery(keyspace, table, MapUtils.EMPTY_MAP, fields);
			SimpleStatement statement = selectQuery.build();
			CqlSession session = connectionManager.getSession(keyspace);
			ResultSet results = session.execute(statement);
			Map<String, String> columnsMapping = CassandraUtil.fetchColumnsMapping(results);
			results.forEach(row -> {
				Map<String, String> rowMap = new HashMap<>();
				columnsMapping.forEach((columnName, internalName) -> {
					Object value = row.getObject(internalName);
					rowMap.put(columnName, value != null ? value.toString() : null);
				});
				String keyValue = rowMap.get(key);
				if (keyValue != null) {
					objectInfo.put(keyValue, rowMap);
				}
			});
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + table + " : " + e.getMessage(), e);
		}
	}

	@Override
	public List<Map<String, Object>> getRecordsWithInClause(String keyspaceName, String tableName, List<Map<String, Object>> propertyMaps, List<String> fields) {
		Select selectQuery = null;
		List<Map<String, Object>> response = new ArrayList<>();
		try {
			if (CollectionUtils.isNotEmpty(fields)) {
				selectQuery = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields);
			} else {
				selectQuery = QueryBuilder.selectFrom(keyspaceName, tableName).all();
			}

			for (Map<String, Object> propertyMap : propertyMaps) {
				for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
					String columnName = entry.getKey();
					Object value = entry.getValue();

					if (value instanceof List) {
						List<?> valueList = (List<?>) value;
						if (CollectionUtils.isNotEmpty(valueList)) {
							List<Term> terms = valueList.stream()
									.map(QueryBuilder::literal)
									.collect(Collectors.toList());
							selectQuery = selectQuery.whereColumn(columnName).in(terms);
						}
					} else {
						selectQuery = selectQuery.whereColumn(columnName).isEqualTo(literal(value));
					}
				}
			}
			SimpleStatement statement = selectQuery.build();
			CqlSession session = connectionManager.getSession(keyspaceName);
			ResultSet results = session.execute(statement);
			response = CassandraUtil.createResponse(results);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	/**
	 * Fetch records with specified columns (select all if null) for given column
	 * map (name, value pairs).
	 *
	 * @param keyspaceName Keyspace name
	 * @param tableName    Table name
	 * @param propertyMap  Map describing columns to be used in where clause of
	 *                     select query.
	 * @param fields       List of columns to be returned in each record
	 * @return List consisting of fetched records
	 */
	public List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName,
			Map<String, Object> propertyMap, List<String> fields) {
		return getRecordsByPropertiesWithoutFiltering(keyspaceName, tableName, propertyMap, fields, null);
	}

	@Override
	public List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName,
			Map<String, Object> propertyMap, List<String> fields, Integer limit) {
		Select selectQuery = null;
		List<Map<String, Object>> response = new ArrayList<>();
		try {
			selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);

			if (limit != null) selectQuery = selectQuery.limit(limit);
			String queryString = selectQuery.toString();
			SimpleStatement statement = SimpleStatement.newInstance(queryString);
			ResultSet results = connectionManager.getSession(keyspaceName).execute(statement);
			response = CassandraUtil.createResponse(results);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	private Select processQueryWithoutFiltering(String keyspaceName, String tableName, Map<String, Object> propertyMap,
			List<String> fields) throws Exception {
		Select selectQuery;
		if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(fields)) {
			selectQuery = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields);
		} else {
			selectQuery = QueryBuilder.selectFrom(keyspaceName, tableName).all();
		}
		if (org.apache.commons.collections4.MapUtils.isNotEmpty(propertyMap)) {
			for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
				String columnName = entry.getKey();
				Object value = entry.getValue();

				if (value instanceof List) {
					List<?> valueList = (List<?>) value;
					if (valueList != null && !valueList.isEmpty()) {
						List<Term> terms = valueList.stream()
								.map(QueryBuilder::literal)
								.collect(Collectors.toList());
						selectQuery = selectQuery.whereColumn(columnName).in(terms);
					}
				} else {
					selectQuery = selectQuery.whereColumn(columnName).isEqualTo(literal(value));
				}
			}
		}
		return selectQuery;
	}

	public Map<String, Object> getRecordsByPropertiesByKey(String keyspaceName, String tableName,
			Map<String, Object> propertyMap, List<String> fields, String key) {
		Map<String, Object> response = new HashMap<>();
		try {
			Select selectQuery = processQueryWithoutFiltering(keyspaceName, tableName, propertyMap, fields);
			SimpleStatement statement = selectQuery.build();
			CqlSession session = connectionManager.getSession(keyspaceName);
			ResultSet results = session.execute(statement);
			response = CassandraUtil.createResponse(results, key);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	@Override
	public List<Map<String, Object>> getKarmaPointsRecordsByPropertiesWithPaginationList(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields, int limit, Date updatedOn, String key,Date limitDate) {
		List<Map<String, Object>> response = new ArrayList<>();
		try {
			Select selectQuery = processQueryWithoutFiltering(keyspaceName, tableName, propertyMap, fields);

			// Add conditions for date range
			selectQuery = selectQuery.whereColumn(Constants.DB_COLUMN_CREDIT_DATE)
					.isLessThan(QueryBuilder.literal(updatedOn));

			if (limitDate != null) {
				selectQuery = selectQuery.whereColumn(Constants.DB_COLUMN_CREDIT_DATE)
						.isGreaterThan(QueryBuilder.literal(limitDate));
			}
			String query = selectQuery.toString();
			query = query.replaceAll(";$", "");
			query = query + " ORDER BY " + Constants.DB_COLUMN_CREDIT_DATE + " DESC LIMIT " + limit;
			SimpleStatement statement = SimpleStatement.newInstance(query);
			ResultSet results = connectionManager.getSession(keyspaceName).execute(statement);
			response = CassandraUtil.createResponse(results);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}
	public Long getRecordCountWithUserId(String keyspace, String tableName, String userId,Date limitDate) {
		try {
			Select selectQuery = QueryBuilder.selectFrom(keyspace, tableName).countAll();
			selectQuery = selectQuery.whereColumn(Constants.USER_ID)
					.isEqualTo(literal(userId));
			selectQuery = selectQuery.whereColumn(Constants.DB_COLUMN_CREDIT_DATE)
					.isGreaterThan(literal(limitDate));
			SimpleStatement statement = selectQuery.build();
			CqlSession session = connectionManager.getSession(keyspace);
			Row row = session.execute(statement).one();
			return row != null ? row.getLong(0) : 0L;
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
			throw e;
		}
	}

	@Override
	public Map<String,Object> getRecordByIdentifierWithPage(String keyspaceName, String tableName, Map<String,Object> key, List<String> fields, String pageString, int limit) {
		Map<String,Object> response = new HashMap<>();
		try {
			CqlSession session = connectionManager.getSession(keyspaceName);
			Select selectQuery;
			if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(fields)) {
				selectQuery = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields);
			} else {
				selectQuery = QueryBuilder.selectFrom(keyspaceName, tableName).all();
			}
			if (org.apache.commons.collections4.MapUtils.isNotEmpty(key)) {
				for (Map.Entry<String, Object> entry : key.entrySet()) {
					if (entry.getValue() instanceof List) {
						List<?> valueList = (List<?>) entry.getValue();
						if (valueList != null && !valueList.isEmpty()) {
							List<Term> terms = valueList.stream()
									.map(QueryBuilder::literal)
									.collect(Collectors.toList());
							selectQuery = selectQuery.whereColumn(entry.getKey()).in(terms);
						}
					} else {
						selectQuery = selectQuery.whereColumn(entry.getKey()).isEqualTo(literal(entry.getValue()));
					}
				}
			}
			SimpleStatement statement = selectQuery.build();
			if (StringUtils.isNotBlank(pageString)) {
				statement = statement.setPagingState(ByteBuffer.wrap(Base64.getDecoder().decode(pageString)));
			}
			statement = statement.setPageSize(limit);
			ResultSet results = session.execute(statement);
			List<Map<String, Object>> responseList = new ArrayList<>();
			Map<String, String> columnsMapping = CassandraUtil.fetchColumnsMapping(results);
			int remaining = results.getAvailableWithoutFetching();
			Iterator<Row> rowIterator = results.iterator();
			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();
				Map<String, Object> rowMap = new HashMap<>();
				for (Map.Entry<String, String> entry : columnsMapping.entrySet()) {
					rowMap.put(entry.getKey(), row.getObject(entry.getValue()));
				}
				responseList.add(rowMap);
				remaining--;
				if (remaining == 0 || responseList.size() >= limit) {
					break;
				}
			}
			response.put(Constants.RESPONSE, responseList);
			ByteBuffer pagingState = results.getExecutionInfo().getPagingState();
			if (pagingState != null) {
				response.put(Constants.PAGE_ID, Base64.getEncoder().encodeToString(pagingState.array()));
			}
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	@Override
	public Long getCountOfRecordByIdentifier(String keyspaceName, String tableName, Map<String, Object> key, String field) {
		Long count = 0L;
		try {
			if (org.apache.commons.collections4.MapUtils.isEmpty(key)) {
				throw new IllegalArgumentException("Key parameter cannot be null");
			}
			// Use Selector.column() instead of QueryBuilder.column()
			Select selectQuery = QueryBuilder.selectFrom(keyspaceName, tableName)
					.function("count", column(field)).as("count");

			for (Entry<String, Object> entry : key.entrySet()) {
				if (entry.getValue() instanceof List) {
					List<?> valueList = (List<?>) entry.getValue();
					if (valueList != null && !valueList.isEmpty()) {
						List<Term> terms = valueList.stream()
								.map(QueryBuilder::literal)
								.collect(Collectors.toList());
						selectQuery = selectQuery.whereColumn(entry.getKey()).in(terms);
					}
				} else {
					selectQuery = selectQuery.whereColumn(entry.getKey())
							.isEqualTo(literal(entry.getValue()));
				}
			}
			CqlSession session = connectionManager.getSession(keyspaceName);
			ResultSet results = session.execute(selectQuery.build());
			Row row = results.one();
			if (row != null) {
				count = row.getLong("count");
			}
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return count;
	}

	private Select processQueryForMultipleInClauses(String keyspaceName, String tableName,
													Map<String, Object> propertyMap, List<String> fields) {

		// Find the first list property to use as the primary IN clause
		Map.Entry<String, Object> primaryListEntry = propertyMap.entrySet().stream()
				.filter(e -> e.getValue() instanceof List && ((List<?>) e.getValue()).size() > 1)
				.findFirst()
				.orElse(null);

		// If no list found, fall back to standard processing
		if (primaryListEntry == null) {
			return processQuery(keyspaceName, tableName, propertyMap, fields);
		}

		// Set up the base query with field selection
		Select selectFrom;
		if (CollectionUtils.isNotEmpty(fields)) {
			selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields.toArray(new String[0]));
		} else {
			selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).all();
		}

		Select selectQuery = selectFrom;

		// Create a modified property map without the primary list property
		Map<String, Object> modifiedPropertyMap = new HashMap<>(propertyMap);
		modifiedPropertyMap.remove(primaryListEntry.getKey());

		// Add all non-list conditions or single-value lists
		for (Entry<String, Object> entry : modifiedPropertyMap.entrySet()) {
			if (entry.getValue() instanceof List) {
				List<?> list = (List<?>) entry.getValue();
				if (CollectionUtils.isNotEmpty(list)) {
					if (list.size() == 1) {
						// For lists with a single value, use equals
						selectQuery = selectQuery.whereColumn(entry.getKey())
								.isEqualTo(QueryBuilder.literal(list.get(0)));
					} else {
						// For other lists, still use IN (but we separated the primary one)
						List<Term> terms = list.stream()
								.map(QueryBuilder::literal)
								.collect(Collectors.toList());
						selectQuery = selectQuery.whereColumn(entry.getKey()).in(terms);
					}
				}
			} else {
				selectQuery = selectQuery.whereColumn(entry.getKey())
						.isEqualTo(QueryBuilder.literal(entry.getValue()));
			}
		}

		// Add the primary list using IN clause
		List<?> primaryList = (List<?>) primaryListEntry.getValue();
		if (CollectionUtils.isNotEmpty(primaryList)) {
			List<Term> terms = primaryList.stream()
					.map(QueryBuilder::literal)
					.collect(Collectors.toList());
			selectQuery = selectQuery.whereColumn(primaryListEntry.getKey()).in(terms);
		}

		// Add filtering directive
		//selectQuery = selectQuery.allowFiltering();

		return selectQuery;
	}
}

