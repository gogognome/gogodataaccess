package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class CurrentTransaction {

    private static final ThreadLocal<CurrentTransaction> threadLocal = new ThreadLocal<CurrentTransaction>() {
        @Override
        protected CurrentTransaction initialValue() {
            return new CurrentTransaction();
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentTransaction.class);

    public static boolean detectOpenTransactions = false;

    private CurrentTransaction() {}

    private final List<Transaction> transactions = new ArrayList<>();

    /* visible for testing */ static CurrentTransaction getInstance() {
        return threadLocal.get();
    }

    public static Supplier<Transaction> transactionCreator = CompositeDatasourceTransaction::new;

    public static Transaction create() {
        Transaction transaction = transactionCreator.get();
        if (detectOpenTransactions && !getInstance().transactions.isEmpty()) {
            LOGGER.warn("Previous transaction still open!");
            getInstance().transactions.get(getInstance().transactions.size() - 1).getCreationDetails();
        }
        getInstance().transactions.add(transaction);
        return transaction;
    }

    private void closeCurrentTransaction() throws DataAccessException {
        int lastTransactionPoolIndex = transactions.size() - 1;
        Transaction transaction = transactions.get(lastTransactionPoolIndex);
        transaction.close();
        transactions.remove(lastTransactionPoolIndex);
    }

    public static Transaction get() throws RuntimeException {
        CurrentTransaction currentTransaction = getInstance();
        try {
            return currentTransaction.transactions.get(currentTransaction.transactions.size() - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalStateException("Trying to get Transaction, but no Transaction has been created.");
        }
    }

    public static void close(boolean commit) throws DataAccessException {
        try {
            Transaction transaction = get();
            if (commit) {
                transaction.commit();
            } else {
                transaction.rollback();
            }
        } finally {
            getInstance().closeCurrentTransaction();
        }
    }

    public static boolean hasTransaction() {
        return !getInstance().transactions.isEmpty();
    }

    public static void logOpenTransactions() {
        LOGGER.info("Open connection pools (oldest first):");
        for (Transaction transaction : getInstance().transactions) {
            LOGGER.info("Next stack trace:" + transaction.getCreationDetails());
        }
    }
}