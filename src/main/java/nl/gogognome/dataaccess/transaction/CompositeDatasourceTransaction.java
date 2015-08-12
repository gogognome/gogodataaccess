package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CompositeDatasourceTransaction extends CompositeTransaction implements JdbcTransaction {

    private final Map<String, Connection> nameToConnection = new HashMap<>(4);

    private final static Map<String, DataSource> NAME_TO_DATA_SOURCE = new HashMap<>();

    public static DataSource getDataSource(String database) throws DataAccessException {
        return NAME_TO_DATA_SOURCE.get(database);
    }

    static public void registerDataSource(String name, DataSource dataSource) {
        NAME_TO_DATA_SOURCE.put(name, dataSource);
    }

    @Override
    public Connection getConnection(Object... parameters) throws SQLException {
        if (parameters.length != 1 || !(parameters[0] instanceof String)) {
            throw new IllegalArgumentException("Parameter must be an array of length 1 containing a String");
        }

        try {
            return getConnection((String) parameters[0]);
        } catch (DataAccessException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            } else {
                throw new SQLException("Could not get connection: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Gets a connection to the database.
     *
     * @param datasourceName
     *            Name of the datasource
     * @return the connection
     * @throws DataAccessException
     */
    public Connection getConnection(String datasourceName) throws DataAccessException {
        Connection connection = nameToConnection.get(datasourceName);
        if (connection != null) {
            return connection;
        }

        try {
            connection = getDataSource(datasourceName).getConnection();
        } catch (SQLException e) {
            throw new DataAccessException("Failed to get connection from data source " + datasourceName, e);
        }

        nameToConnection.put(datasourceName, connection);
        addTransaction(new ConnectionTransaction(connection));

        try {
            if (connection.getTransactionIsolation() != Connection.TRANSACTION_READ_COMMITTED) {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new DataAccessException("Failed to configure the connection for data source " + datasourceName, e);
        }
        return connection;
    }
}