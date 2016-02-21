package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

public abstract class NewTransaction {

    public static void runs(RunnableWithoutReturnValue runnable) throws DataAccessException {
        returns(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T returns(RunnableWithReturnValue<T> runnable) throws DataAccessException {
        T result = null;
        boolean commitOnClose = false;
        try {
            CurrentTransaction.create();
            result = runnable.run();
            commitOnClose = true;
        } catch (Exception e) {
            TransactionExceptionHandler.handleException(e);
        } finally {
            CurrentTransaction.close(commitOnClose);
        }

        return result;
    }

}
