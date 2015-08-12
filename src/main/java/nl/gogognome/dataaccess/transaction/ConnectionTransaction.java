package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.util.CreationStack;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionTransaction implements Transaction {

    private final Connection connection;
    private final CreationStack creationStack;

    public ConnectionTransaction(Connection connection) {
        if (TransactionSettings.storeCreationStackForTransactions) {
            creationStack = new CreationStack();
        } else {
            creationStack = null;
        }
        this.connection = connection;
    }

    @Override
    public void commit() throws DataAccessException {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new DataAccessException("Failed to commit: " + e.getMessage(), e);
        }
    }

    @Override
    public void rollback() throws DataAccessException {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new DataAccessException("Failed to rollback: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws DataAccessException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DataAccessException("Failed to close: " + e.getMessage(), e);
        }
    }

    public String getCreationDetails() {
        if (creationStack != null) {
            return creationStack.toString();
        } else {
            return "Creation stacks are not stored.";
        }
    }
}