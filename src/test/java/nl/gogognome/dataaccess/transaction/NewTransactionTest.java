package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NewTransactionTest {

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
    public void withoutResultShouldBeRunInsideTransaction() {
        NewTransaction.runs(() -> calledMethods.append("runs;"));

        assertEquals("transaction creation;runs;commit;close;", calledMethods.toString());
    }


    @Test
    public void nestedwithoutResultShouldBeRunInsideNestedTransactions() {
        NewTransaction.runs(() -> {
            NewTransaction.runs(() -> calledMethods.append("runs;"));
        });

        assertEquals("transaction creation;transaction creation;runs;commit;close;commit;close;", calledMethods.toString());
    }

    @Test
    public void withoutResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() {
        try {
            NewTransaction.runs(() -> {
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
    public void withResultShouldBeRunInsideTransaction() {
        String result = NewTransaction.returns(() -> {
            calledMethods.append("runs;");
            return "test";
        });

        assertEquals("test", result);
        assertEquals("transaction creation;runs;commit;close;", calledMethods.toString());
    }

    @Test
    public void nestedWithResultShouldBeRunInsideNestedTransactions() {
        String result = NewTransaction.returns(() -> NewTransaction.returns(() -> {
            calledMethods.append("runs;");
            return "test";
        }));

        assertEquals("test", result);
        assertEquals("transaction creation;transaction creation;runs;commit;close;commit;close;", calledMethods.toString());
    }

    @Test
    public void withResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() {
        try {
            NewTransaction.returns(() -> {
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
        public void commit() {
            calledMethods.append("commit;");
        }

        @Override
        public void rollback() {
            calledMethods.append("rollback;");
        }

        @Override
        public void close() {
            calledMethods.append("close;");
        }

        @Override
        public String getCreationDetails() {
            return null;
        }
    }
}