package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;

import java.sql.SQLException;

public class RequireTransaction {

    public static void runs(RunnableWithoutReturnValue runnable) throws DataAccessException {
        returns(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T returns(RunnableWithReturnValue<T> runnable) throws DataAccessException {
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
            TransactionExceptionHandler.handleException(e);
        } finally {
            if (!runInsideExistingTransaction) {
                CurrentTransaction.close(commitOnClose);
            }
        }

        return result;
    }

}
