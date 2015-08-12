package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.*;

public class CurrentTransactionTest {

    @Test
    public void multipleThreads_eachThreadGetsItsOwnInstanceOfCurrentTransaction() throws Exception {
        CurrentTransactionCheckThread[] threads = new CurrentTransactionCheckThread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new CurrentTransactionCheckThread(1000);
            threads[i].start();
        }

        for (CurrentTransactionCheckThread thread : threads) {
            thread.join();
            assertNull(thread.getThrowable());
        }
    }

    @Test
    public void transactionShouldBeClosedEvenIfTransactionThrowsExceptionOnCommit() throws Exception {
        CurrentTransaction.create();
        Supplier<Transaction> oldTransactionCreator = CurrentTransaction.transactionCreator;
        try {
            CurrentTransaction.transactionCreator = () -> new Transaction() {

                @Override
                public void commit() throws DataAccessException {
                    throw new DataAccessException("commit failed");
                }

                @Override
                public void rollback() throws DataAccessException {
                    throw new DataAccessException("commit failed");
                }

                @Override
                public void close() throws DataAccessException {
                    throw new DataAccessException("commit failed");
                }

                @Override
                public String getCreationDetails() {
                    return "...";
                }
            };

            try {
                CurrentTransaction.close(true);
            } catch (Exception e) {
                // ignore exception
            }
        } finally {
            CurrentTransaction.transactionCreator = oldTransactionCreator;
        }

        assertFalse(CurrentTransaction.hasTransaction());
    }
}

class CurrentTransactionCheckThread extends Thread {

    private final long endTime;
    private Throwable throwable;

    public CurrentTransactionCheckThread(int durationMs) {
        endTime = System.currentTimeMillis() + durationMs;
    }

    @Override
    public void run() {
        try {
            CurrentTransaction.create();
            CurrentTransaction currentTransactionPool = CurrentTransaction.getInstance();
            CurrentTransaction.close(false);
            while (System.currentTimeMillis() < endTime) {
                for (int i = 0; i < 1000; i++) {
                    CurrentTransaction.create();
                    assertSame(currentTransactionPool, CurrentTransaction.getInstance());
                    CurrentTransaction.close(false);
                }
            }
        } catch (Throwable e) {
            throwable = e;
        }
    }

    public Throwable getThrowable() {
        return throwable;
    }
}
