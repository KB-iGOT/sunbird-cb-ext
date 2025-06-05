package org.sunbird.cassandra.utils;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.sunbird.common.util.Constants;

import java.util.*;

/**
 * @author fathima
 * @desc This class will provide all required helper method for cassandra db operation.
 */
public final class CassandraUtil {

    private static CassandraPropertyReader propertiesCache = CassandraPropertyReader.getInstance();

    /**
     * @param keyspaceName Keyspace name
     * @param tableName    Table name
     * @param map          Map where key is column name and value is column value
     * @return Prepared statement
     * @desc This method is used to create prepared statement based on table name and column name
     * provided in request
     */
    public static String getPreparedStatement(
            String keyspaceName, String tableName, Map<String, Object> map) {
        StringBuilder query = new StringBuilder();
        query.append(
                Constants.INSERT_INTO + keyspaceName + Constants.DOT + tableName + Constants.OPEN_BRACE);
        Set<String> keySet = map.keySet();
        query.append(String.join(",", keySet) + Constants.VALUES_WITH_BRACE);
        StringBuilder commaSepValueBuilder = new StringBuilder();
        for (int i = 0; i < keySet.size(); i++) {
            commaSepValueBuilder.append(Constants.QUE_MARK);
            if (i != keySet.size() - 1) {
                commaSepValueBuilder.append(Constants.COMMA);
            }
        }
        query.append(commaSepValueBuilder + Constants.CLOSING_BRACE);
        return query.toString();
    }

    /**
     * @param results ResultSet
     * @return Response Response
     * @desc This method is used for creating response from the resultset i.e return map
     * <String,Object> or map<columnName,columnValue>
     */
    public static List<Map<String, Object>> createResponse(ResultSet results) {
        List<Map<String, Object>> responseList = new ArrayList<>();
        Map<String, String> columnsMapping = fetchColumnsMapping(results);
        Iterator<Row> rowIterator = results.iterator();
        rowIterator.forEachRemaining(
                row -> {
                    Map<String, Object> rowMap = new HashMap<>();
                    columnsMapping
                            .entrySet()
                            .stream()
                            .forEach(entry -> rowMap.put(entry.getKey(), row.getObject(entry.getValue())));
                    responseList.add(rowMap);
                });
        return responseList;
    }

    public static Map<String, Object> createResponse(ResultSet results, String key) {
        Map<String, Object> responseList = new HashMap<>();
        Map<String, String> columnsMapping = fetchColumnsMapping(results);
        Iterator<Row> rowIterator = results.iterator();
        rowIterator.forEachRemaining(
                row -> {
                    Map<String, Object> rowMap = new HashMap<>();
                    columnsMapping
                            .entrySet()
                            .stream()
                            .forEach(entry -> {
                                rowMap.put(entry.getKey(), row.getObject(entry.getValue()));
                            });

                    responseList.put((String) rowMap.get(key), rowMap);
                });
        return responseList;
    }

    public static Map<String, String> fetchColumnsMapping(ResultSet results) {
        Map<String, String> columnsMapping = new HashMap<>();
        results.getColumnDefinitions().forEach(column -> {
            String property = propertiesCache.readProperty(column.getName().asInternal()).trim();
            columnsMapping.put(property, column.getName().asInternal());
        });
        return columnsMapping;
    }
}
