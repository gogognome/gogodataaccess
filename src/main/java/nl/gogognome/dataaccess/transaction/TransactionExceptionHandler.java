package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

import java.sql.SQLException;

class TransactionExceptionHandler {

    static void handleException(Exception e) throws DataAccessException {
        if (e instanceof DataAccessException) {
            throw (DataAccessException) e;
        } else {
            String message = e.getLocalizedMessage();
            if (message == null) {
                message = e.getClass().getSimpleName();
            }
            throw new DataAccessException(message, e);
        }
    }

}
