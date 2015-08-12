package nl.gogognome.dataaccess.dao;

import nl.gogognome.dataaccess.transaction.CurrentTransaction;
import nl.gogognome.dataaccess.transaction.JdbcTransaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class QueryBuilder {

    private final Object[] connectionParameters;
    private String sqlStatement;
    private Object[] parameters;

    public QueryBuilder(Object[] connectionParameters) {
        this.connectionParameters = connectionParameters;
    }

    public QueryBuilder execute(String sqlStatement, Object... parameters) {
        this.sqlStatement = sqlStatement;
        this.parameters = parameters;
        return this;
    }

    /**
     * Executes the statement and returns the a value based on the first result.
     *
     * @param converter converts the result set to the return value
     * @param <T> the type of the returned value
     * @return the value based on the first result
     * @throws SQLException if a problem occurs
     * @throws NoRecordFoundException if the statement did not return any result
     */
    public <T> T getFirst(ResultSetConverter<T> converter) throws SQLException {
        try (PreparedStatementWrapper statement = prepareStatement(sqlStatement, parameters)) {
            ResultSetWrapper result = statement.executeQuery();
            if (result.next()) {
                return converter.convert(result);
            }
            else {
                throw new NoRecordFoundException("No result returned by query.");
            }
        }
    }

    /**
     * Executes the statement and returns the a value based on the first result.
     *
     * @param converter converts the result set to the return value
     * @param <T> the type of the returned value
     * @return the value based on the first result or null if the statement dit not return any result
     * @throws SQLException if a problem occurs
     */
    public <T> T findFirst(ResultSetConverter<T> converter) throws SQLException {
        try (PreparedStatementWrapper statement = prepareStatement(sqlStatement, parameters)) {
            ResultSetWrapper result = statement.executeQuery();
            return result.next() ? converter.convert(result) : null;
        }
    }

    /**
     * Executes the statement and calls the consumer for each result.
     *
     * @param consumer is called for each result
     * @throws SQLException if a problem occurs
     */
    public void forEach(ResultSetConsumer consumer) throws SQLException {
        try (PreparedStatementWrapper statement = prepareStatement(sqlStatement, parameters)) {
            ResultSetWrapper result = statement.executeQuery();
            while (result.next()) {
                consumer.consume(result);
            }
        }
    }

    /**
     * Executes the statement and adds the results to the list.
     *
     * @param list the list
     * @param converter converts each result before adding it to the list
     * @param <T> the type of the list elements
     * @return the list
     * @throws SQLException if a problem occurs
     */
    public <T> List<T> addToList(List<T> list, ResultSetConverter<T> converter) throws SQLException {
        forEach(result -> list.add(converter.convert(result)));
        return list;
    }

    /**
     * Executes the statement and converts the results to a list.
     *
     * @param converter converts each result before adding it to the list
     * @param <T> the type of the list elements
     * @return the list
     * @throws SQLException if a problem occurs
     */
    public <T> List<T> toList(ResultSetConverter<T> converter) throws SQLException {
        return addToList(new ArrayList<>(50), converter);
    }

    /**
     * Executes the statement and adds the results to the set.
     *
     * @param set the set
     * @param converter converts each result before adding it to the set
     * @param <T> the type of the set elements
     * @return the set
     * @throws SQLException if a problem occurs
     */
    public <T> Set<T> addToSet(Set<T> set, ResultSetConverter<T> converter) throws SQLException {
        forEach(result -> set.add(converter.convert(result)));
        return set;
    }

    /**
     * Executes the statement and converts the results to a set.
     *
     * @param converter converts each result before adding it to the set
     * @param <T> the type of the set elements
     * @return the set
     * @throws SQLException if a problem occurs
     */
    public <T> Set<T> toSet(ResultSetConverter<T> converter) throws SQLException {
        return addToSet(new HashSet<>(100), converter);
    }

    /**
     * Executes the statement and adds an key-value pair to the map for each result.
     *
     * @param map the map
     * @param keyConverter converts a result to a key
     * @param valueConverter converts a result to a value
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @param <M> the type of the map
     * @return the map
     * @throws SQLException if a problem occurs
     */
    public<K, V, M extends Map<K, V>> M addToMap(M map, ResultSetConverter<K> keyConverter, ResultSetConverter<V> valueConverter) throws SQLException {
        forEach(result -> map.put(keyConverter.convert(result), valueConverter.convert(result)));
        return map;
    }

    /**
     * Executes the statement and adds the result to a HashMap.
     * @param keyConverter converts a result to a key
     * @param valueConverter converts a result to a value
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the HashMap
     * @throws SQLException if a problem occurs
     */
    public <K,V> HashMap<K,V> toHashMap(ResultSetConverter<K> keyConverter, ResultSetConverter<V> valueConverter) throws SQLException {
        return addToMap(new HashMap<>(50), keyConverter, valueConverter);
    }

    /**
     * Executes the statement and adds the result to a TreeMap.
     * @param keyConverter converts a result to a key
     * @param valueConverter converts a result to a value
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the TreeMap
     * @throws SQLException if a problem occurs
     */
    public <K,V> TreeMap<K,V> toTreeMap(ResultSetConverter<K> keyConverter, ResultSetConverter<V> valueConverter) throws SQLException {
        return addToMap(new TreeMap<>(), keyConverter, valueConverter);
    }

    /**
     * Executes the statement andd adds the result to a map of lists.
     *
     * @param map the map
     * @param keyConverter converts a result to a key
     * @param valueConverter converts a result to a value
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @param <M> the type of the map
     * @return the map
     * @throws SQLException if a problem occurs
     */
    public <K, V, M extends Map<K, List<V>>> M addToMapOfLists(M map, ResultSetConverter<K> keyConverter, ResultSetConverter<V> valueConverter)
            throws SQLException {
        forEach(result -> {
                    K key = keyConverter.convert(result);
                    if (!map.containsKey(key)) {
                        map.put(key, new ArrayList<>());
                    }
                    map.get(key).add(valueConverter.convert(result));
                });
        return map;
    }

    /**
     * Executes the statement and adds the result to a HashMap of lists.
     *
     * @param keyConverter converts a result to a key
     * @param valueConverter converts a result to a value
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the HashMap
     * @throws SQLException if a problem occurs
     */
    public <K,V> HashMap<K,List<V>> toHashMapOfLists(ResultSetConverter<K> keyConverter, ResultSetConverter<V> valueConverter) throws SQLException {
        return addToMapOfLists(new HashMap<>(50), keyConverter, valueConverter);
    }

    /**
     * Executes the statement and adds the result to a TreeMap of lists.
     * @param keyConverter converts a result to a key
     * @param valueConverter converts a result to a value
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the TreeMap
     * @throws SQLException if a problem occurs
     */
    public <K,V> TreeMap<K,List<V>> toTreeMapOfLists(ResultSetConverter<K> keyConverter, ResultSetConverter<V> valueConverter) throws SQLException {
        return addToMapOfLists(new TreeMap<>(), keyConverter, valueConverter);
    }

    /**
     * Executes the statement and checks whether at least one result is returned by the statement.
     * @return true if at least one result is returned; false if no result is returned
     * @throws SQLException if a problem occurs
     */
    public boolean exists() throws SQLException {
        try (PreparedStatementWrapper statement = prepareStatement(sqlStatement, parameters)) {
            ResultSetWrapper result = statement.executeQuery();
            return result.next();
        }
    }

    public int getNumberModifiedRows() throws SQLException {
        try (PreparedStatementWrapper statement = prepareStatement(sqlStatement, parameters)) {
            return statement.executeUpdate();
        }
    }


    public void ignoreResult() throws SQLException {
        try (PreparedStatementWrapper statement = prepareStatement(sqlStatement, parameters)) {
            statement.execute();
        }
    }

    /**
     * Creates a prepared statement. This method actually returns a {@link PreparedStatementWrapper}, which has some advantages over using a regular
     * {@link PreparedStatement}.
     *
     * @param query
     *            the query
     * @return the prepared statement
     * @throws SQLException
     *             if a problem occurs
     */
    private PreparedStatementWrapper prepareStatement(String query, Object... parameters) throws SQLException {
        Connection connection = ((JdbcTransaction) CurrentTransaction.get()).getConnection(connectionParameters);
        return PreparedStatementWrapper.preparedStatement(connection, query, parameters);
    }
}
