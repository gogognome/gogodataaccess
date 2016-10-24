package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

/**
 * This interface specifies a transaction. A transaction is started by creating an instance of Transaction.
 * To finish the transaction either call commit() or rollback().
 * Finally, close() must be called on the transaction. A Transaction instance can be used for one transaction.
 */
public interface Transaction {

    void commit() throws DataAccessException;

    void rollback() throws DataAccessException;

    void close() throws DataAccessException;

    String getCreationDetails();

}
