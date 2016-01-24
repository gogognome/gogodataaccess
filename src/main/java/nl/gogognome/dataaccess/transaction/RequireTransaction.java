package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

import java.sql.SQLException;

public class RequireTransaction {

    public static void withoutResult(RunnableWithoutReturnValue runnable) throws DataAccessException {
        withResult(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T withResult(RunnableWithReturnValue<T> runnable) throws DataAccessException {
        T result = null;
        boolean runInsideExistingTransaction = CurrentTransaction.hasTransaction();
        boolean commitOnClose = false;
        try {
            if (!runInsideExistingTransaction) {
                CurrentTransaction.create();
            }
            result = runnable.run();
            commitOnClose = true;
        } catch (Exception e) {
            handleException(e);
        } finally {
            if (!runInsideExistingTransaction) {
                CurrentTransaction.close(commitOnClose);
            }
        }

        return result;
    }

    private static void handleException(Exception e) throws DataAccessException {
        if (e instanceof DataAccessException) {
            throw (DataAccessException) e;
        } else if (e instanceof SQLException) {
            throw new DataAccessException(e.getMessage(), e);
        } else {
            String message = e.getLocalizedMessage();
            if (message == null) {
                message = e.getClass().getSimpleName();
            }
            throw new DataAccessException(message, e);
        }
    }

}
