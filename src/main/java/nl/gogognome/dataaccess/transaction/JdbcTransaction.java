package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcTransaction {

    /**
     * Get a connection with the specified parameters.
     * The first call with certain parameters within the current transaction will create a new connection.
     * Subsequent calls with equal parameters within the current transaction will return the same connection.
     * Callers of this method do not have to cache the connection themselves.
     * @param parameters implementation dependent parameters
     * @return a conenction instance
     * @throws SQLException if the connection could not be created or another problem occurs with the connection
     */
    Connection getConnection(Object... parameters) throws SQLException;

}
