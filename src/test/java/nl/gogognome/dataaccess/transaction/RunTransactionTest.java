package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class RunTransactionTest {

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
        RunTransaction.withoutResult(() -> calledMethods.append("run;"));

        assertEquals("transaction creation;run;commit;close;", calledMethods.toString());
    }

    @Test
    public void withoutResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() throws DataAccessException {
        try {
            RunTransaction.withoutResult(() -> {
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
        String result = RunTransaction.withResult(() -> {
            calledMethods.append("run;");
            return "test";
        });

        assertEquals("test", result);
        assertEquals("transaction creation;run;commit;close;", calledMethods.toString());
    }

    @Test
    public void withResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() throws DataAccessException {
        try {
            RunTransaction.withResult(() -> {
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
    public void readonlyShouldBeRunInsideTransaction() throws DataAccessException {
        String result = RunTransaction.readOnly(() -> {
            calledMethods.append("run;");
            return "test";
        });

        assertEquals("test", result);
        assertEquals("transaction creation;run;rollback;close;", calledMethods.toString());
    }

    @Test
    public void readonlyThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() throws DataAccessException {
        try {
            RunTransaction.readOnly(() -> {
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
    public void testHandlingOfSQLException() throws DataAccessException {
        try {
            RunTransaction.readOnly(() -> {
                calledMethods.append("throw exception;");
                throw new SQLException("test");
            });
            fail("Expected exception was not thrown!");
        } catch (DataAccessException e) {
            assertEquals("test", e.getMessage());
            assertTrue(e.getCause() instanceof SQLException);
        }
    }

    @Test
    public void testHandlingOfOtherExceptionWithMessage() throws DataAccessException {
        try {
            RunTransaction.readOnly(() -> {
                calledMethods.append("throw exception;");
                throw new IllegalArgumentException("test");
            });
            fail("Expected exception was not thrown!");
        } catch (DataAccessException e) {
            assertEquals("test", e.getMessage());
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testHandlingOfOtherExceptionWithoutMessage() throws DataAccessException {
        try {
            RunTransaction.readOnly(() -> {
                calledMethods.append("throw exception;");
                throw new IllegalArgumentException();
            });
            fail("Expected exception was not thrown!");
        } catch (DataAccessException e) {
            assertEquals("IllegalArgumentException", e.getMessage());
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
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