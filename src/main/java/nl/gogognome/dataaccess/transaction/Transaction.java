package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

public interface Transaction {

    void commit() throws DataAccessException;

    void rollback() throws DataAccessException;

    void close() throws DataAccessException;

    String getCreationDetails();

}
