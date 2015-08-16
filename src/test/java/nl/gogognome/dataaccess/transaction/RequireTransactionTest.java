package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RequireTransactionTest {

    private Supplier<Transaction> oldTransactionCreator;
    private StringBuilder calledMethods = new StringBuilder();

    @Before
    public void setup() {
        oldTransactionCreator = CurrentTransaction.transactionCreator;
        CurrentTransaction.transactionCreator = TransactionMock::new;
    }

    @After
    public void teardown() {
        CurrentTransaction.transactionCreator = oldTransactionCreator;
    }

    @Test
    public void withoutResultShouldBeRunInsideTransaction() throws DataAccessException {
        RequireTransaction.withoutResult(() -> calledMethods.append("run;"));

        assertEquals("transaction creation;run;commit;close;", calledMethods.toString());
    }


    @Test
    public void nestedwithoutResultShouldBeRunInsideNestedTransactions() throws DataAccessException {
        RequireTransaction.withoutResult(() -> {
            RequireTransaction.withoutResult(() -> calledMethods.append("run;"));
        });

        assertEquals("transaction creation;run;commit;close;", calledMethods.toString());
    }

    @Test
    public void withoutResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() throws DataAccessException {
        try {
            RequireTransaction.withoutResult(() -> {
                calledMethods.append("throw exception;");
                throw new DataAccessException();
            });
            fail("Expected exception was not thrown!");
        } catch (DataAccessException e) {
            // expected result
        }
        assertEquals("transaction creation;throw exception;rollback;close;", calledMethods.toString());
    }

    @Test
    public void withResultShouldBeRunInsideTransaction() throws DataAccessException {
        String result = RequireTransaction.withResult(() -> {
            calledMethods.append("run;");
            return "test";
        });

        assertEquals("test", result);
        assertEquals("transaction creation;run;commit;close;", calledMethods.toString());
    }

    @Test
    public void nestedWithResultShouldBeRunInsideNestedTransactions() throws DataAccessException {
        String result = RequireTransaction.withResult(() -> {
            return RequireTransaction.withResult(() -> {
                calledMethods.append("run;");
                return "test";
            });
        });

        assertEquals("test", result);
        assertEquals("transaction creation;run;commit;close;", calledMethods.toString());
    }

    @Test
    public void withResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() throws DataAccessException {
        try {
            RequireTransaction.withResult(() -> {
                calledMethods.append("throw exception;");
                throw new DataAccessException();
            });
            fail("Expected exception was not thrown!");
        } catch (DataAccessException e) {
            // expected result
        }

        assertEquals("transaction creation;throw exception;rollback;close;", calledMethods.toString());
    }

    class TransactionMock implements Transaction {

        public TransactionMock() {
            calledMethods.append("transaction creation;");
        }

        @Override
        public void commit() throws DataAccessException {
            calledMethods.append("commit;");
        }

        @Override
        public void rollback() throws DataAccessException {
            calledMethods.append("rollback;");
        }

        @Override
        public void close() throws DataAccessException {
            calledMethods.append("close;");
        }

        @Override
        public String getCreationDetails() {
            return null;
        }
    }
}