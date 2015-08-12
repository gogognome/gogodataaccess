package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

import java.sql.SQLException;

public abstract class RunTransaction {

    public static void withoutResult(RunnableWithoutReturnValue runnable) throws DataAccessException {
        withResult(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T readOnly(RunnableWithReturnValue<T> runnable) throws DataAccessException {
        T result = null;
        try {
            CurrentTransaction.create();
            result = runnable.run();
        } catch (Exception e) {
            handleException(e);
        } finally {
            CurrentTransaction.close(false);
        }

        return result;
    }

    public static <T> T withResult(RunnableWithReturnValue<T> runnable) throws DataAccessException {
        T result = null;
        boolean commitOnClose = false;
        try {
            CurrentTransaction.create();
            result = runnable.run();
            commitOnClose = true;
        } catch (Exception e) {
            handleException(e);
        } finally {
            CurrentTransaction.close(commitOnClose);
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
