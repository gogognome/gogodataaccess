package nl.gogognome.dataaccess.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This class wraps a {@link PreparedStatement}. This class has two advantages over using the {@link PreparedStatement} directly:
 *
 * <ul>
 * <li>Its {@link #toString()} method returns the query that was used to create the statement
 * <li>Its finalizer logs a message if the statement was not closed
 * </ul>
 */
public class PreparedStatementWrapper implements AutoCloseable {

    private final static Logger LOGGER = LoggerFactory.getLogger(PreparedStatementWrapper.class);

    private final PreparedStatement wrappedStatement;
    private final String query;
    private boolean closed;
    private long startTimeNano;

    private final List<Object> parameters = new ArrayList<>(10);
    private List<List<Object>> batchParameters;

    /**
     * Constructor.
     *
     * @param wrappedStatement
     *            the wrapped statement
     * @param query
     *            the query used to create the wrapped statement. Used for logging only.
     * @param startTimeNano
     *            the creation time (in nanoseconds) of the prepared statement
     */
    private PreparedStatementWrapper(PreparedStatement wrappedStatement, String query, long startTimeNano) {
        super();
        this.wrappedStatement = wrappedStatement;
        this.query = query;
        this.startTimeNano = startTimeNano;
        try {
            wrappedStatement.setFetchSize(100);
        } catch (SQLException e) {
            LOGGER.warn("Ignored exception while setting fetch size: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a prepared statement
     *
     * @param connection
     *            the connection
     * @param query
     *            the query
     * @return the prepared statement
     * @throws SQLException
     *             if a problem occurs
     */
    public static PreparedStatementWrapper preparedStatement(Connection connection, String query, Object... parameters) throws SQLException {
        long startTimeNano = System.nanoTime();

        query = fillInPivots(query, parameters);
        PreparedStatement wrappedStatement = connection.prepareStatement(query);
        PreparedStatementWrapper wrapper = new PreparedStatementWrapper(wrappedStatement, query, startTimeNano);
        wrapper.setQueryParameters(parameters);
        return wrapper;
    }

    private static String fillInPivots(String query, Object[] parameters) throws SQLException {
        for (Object parameter : parameters) {
            if (parameter instanceof Iterable) {
                query = query.replaceFirst("[(][?][)]", DAOUtil.convertToPivotClause((Iterable) parameter));
            }
        }
        return query;
    }

    private void setQueryParameters(Object... parameters) throws SQLException {
        int parameterPosition = 1;
        for (Object parameter : parameters) {
            if (!(parameter instanceof Iterable)) {
                this.setObject(parameterPosition, parameter);
                parameterPosition++;
            }
        }
    }

    public void addBatch() throws SQLException {
        wrappedStatement.addBatch();
        if (batchParameters == null) {
            batchParameters = new ArrayList<>();
        }
        batchParameters.add(new ArrayList<>(parameters));
        parameters.clear();
    }

    public boolean execute() throws SQLException {
        logStatement();
        try {
            return wrappedStatement.execute();
        } catch (SQLException e) {
            throwModifiedException(e);
            return false; // unreachable code
        } finally {
            long endTimeNano = System.nanoTime();
            logStatement(startTimeNano, endTimeNano);
        }
    }

    public ResultSetWrapper executeQuery() throws SQLException {
        logStatement();
        try {
            return new ResultSetWrapper(wrappedStatement.executeQuery());
        } catch (SQLException e) {
            throwModifiedException(e);
            return new ResultSetWrapper(null); // unreachable code. Null is not allowed as return value
        } finally {
            long endTimeNano = System.nanoTime();
            logStatement(startTimeNano, endTimeNano);
        }
    }

    public int executeUpdate() throws SQLException {
        logStatement();
        try {
            return wrappedStatement.executeUpdate();
        } catch (SQLException e) {
            throwModifiedException(e);
            return -1; // unreachable code
        } finally {
            long endTimeNano = System.nanoTime();
            logStatement(startTimeNano, endTimeNano);
        }
    }

    private void logStatement() {
    }

    private void throwModifiedException(SQLException e) throws SQLException {
        throwExceptionIncludingStatement(e);
    }

    private void throwExceptionIncludingStatement(SQLException e) throws SQLException {
        throw new SQLException(e.getMessage() + " in query " + toString(), e.getSQLState(), e.getErrorCode(), e);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setBigDecimal(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setDate(parameterIndex, x);
    }
    public void setDouble(int parameterIndex, double x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setDouble(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setFloat(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setLong(parameterIndex, x);
    }
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        addParameter(parameterIndex, null);
        wrappedStatement.setNull(parameterIndex, sqlType);
    }
    public void setObject(int parameterIndex, Object x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setObject(parameterIndex, x);
    }
    public void setString(int parameterIndex, String x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setString(parameterIndex, x);
    }
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        addParameter(parameterIndex, x);
        wrappedStatement.setTimestamp(parameterIndex, x);
    }

    @Override
    public void close() throws SQLException {
        wrappedStatement.close();
        closed = true;
    }
    public int[] executeBatch() throws SQLException {
        logStatement();
        try {
            return wrappedStatement.executeBatch();
        } catch (SQLException e) {
            throwModifiedException(e);
            return null; // unreachable code
        } finally {
            long endTimeNano = System.nanoTime();
            logStatement(startTimeNano, endTimeNano);
        }
    }

    public Connection getConnection() throws SQLException {
            return wrappedStatement.getConnection();
    }

    /**
     * Logs a statement.
     *
     * @param start
     *            the start time in nanoseconds
     * @param end
     *            the end time in nanoseconds
     */
    private void logStatement(long start, long end) {
        long ms = (end - start) / 1000000;
        if (ms < 100) {
            LOGGER.trace(toString() + " took " + ms + " ms");
        } else if (ms < 1000) {
            LOGGER.debug(toString() + " took " + ms + " ms");
        } else if (ms < 10000) {
            LOGGER.info(toString() + " took " + ms + " ms");
        } else {
            LOGGER.warn(toString() + " took " + ms + " ms");
        }

        // Reset start time for the case this statement is used again.
        startTimeNano = end;
    }

    /**
     * Stores the parameter in the {@link #parameters} list.
     *
     * @param index
     *            the index of the parameter in the query
     * @param parameter
     *            the parameter
     */
    private void addParameter(int index, Object parameter) {
        index -= 1; // parameter indexes start at 1 instead of 0.
        if (parameters.size() > index) {
            parameters.set(index, parameter);
        } else {
            while (parameters.size() < index) {
                parameters.add(null);
            }
            parameters.add(parameter);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            LOGGER.error("PreparedStatement " + this + " was not closed before finalization!");
            close();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(query.length() + 100);
        if (batchParameters == null) {
            appendQueryIncludingParameters(sb, parameters);
        } else {
            boolean first = true;
            for (List<Object> tempParameters : batchParameters) {
                if (!first) {
                    sb.append("; ");
                }
                first = false;

                appendQueryIncludingParameters(sb, tempParameters);
            }
        }
        return sb.toString();
    }

    private void appendQueryIncludingParameters(StringBuilder sb, List<Object> parameters) {
        int paramIndex = 0;
        int index = query.indexOf('?');
        int prevIndex = 0;
        while (index != -1) {
            sb.append(query.substring(prevIndex, index));
            if (paramIndex < parameters.size()) {
                Object value = parameters.get(paramIndex);
                if (value instanceof String) {
                    sb.append("'").append(value).append("'");
                } else {
                    sb.append(value);
                }
            } else {
                sb.append('?');
            }
            paramIndex += 1;
            prevIndex = index + 1;
            index = query.indexOf('?', prevIndex);
        }
        sb.append(query.substring(prevIndex));
    }
}
