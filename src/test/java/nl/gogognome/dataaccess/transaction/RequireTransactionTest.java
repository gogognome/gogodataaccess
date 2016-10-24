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
    public void withoutResultShouldBeRunInsideTransaction() {
        RequireTransaction.runs(() -> calledMethods.append("runs;"));

        assertEquals("transaction creation;runs;commit;close;", calledMethods.toString());
    }


    @Test
    public void nestedwithoutResultShouldBeRunInsideNestedTransactions() {
        RequireTransaction.runs(() -> RequireTransaction.runs(() -> calledMethods.append("runs;")));

        assertEquals("transaction creation;runs;commit;close;", calledMethods.toString());
    }

    @Test
    public void withoutResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() {
        try {
            RequireTransaction.runs(() -> {
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
        String result = RequireTransaction.returns(() -> {
            calledMethods.append("runs;");
            return "test";
        });

        assertEquals("test", result);
        assertEquals("transaction creation;runs;commit;close;", calledMethods.toString());
    }

    @Test
    public void nestedWithResultShouldBeRunInsideNestedTransactions() {
        String result = RequireTransaction.returns(() -> RequireTransaction.returns(() -> {
            calledMethods.append("runs;");
            return "test";
        }));

        assertEquals("test", result);
        assertEquals("transaction creation;runs;commit;close;", calledMethods.toString());
    }

    @Test
    public void withResultThatThrowsExceptionShouldBeRunInsideTransactionThatIsRolledBack() {
        try {
            RequireTransaction.returns(() -> {
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
    public void createNewTransactionWhileClosingAnotherTransactionShouldSucceed() {
        CurrentTransaction.transactionCreator = () -> new TransactionMock() {
            @Override
            public void close() {
                super.close();
                if (calledMethods.indexOf("runs2") == -1) {
                    RequireTransaction.runs(() -> calledMethods.append("runs2;"));
                }
            }
        };
        RequireTransaction.runs(() -> calledMethods.append("runs1;"));
        assertEquals("transaction creation;runs1;commit;close;transaction creation;runs2;commit;close;", calledMethods.toString());
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