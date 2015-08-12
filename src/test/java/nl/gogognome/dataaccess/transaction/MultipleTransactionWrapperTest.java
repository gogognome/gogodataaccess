package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MultipleTransactionWrapperTest {

    private final static int NR_TRANSACTIONS = 10;
    private Transaction[] transactions;
    private final CompositeTransaction transactionWrapper = new CompositeTransaction();

    @Before
    public void initTransactionsAndAddThemToThePool() {
        transactions = new Transaction[NR_TRANSACTIONS];
        for (int i = 0; i < NR_TRANSACTIONS; i++) {
            transactions[i] = mock(Transaction.class);
        }

        for (Transaction t : transactions) {
            transactionWrapper.addTransaction(t);
        }
    }

    @Test
    public void commitMultipleTransactions_oneThrowsException_allTransactionsHaveBeenCommitted() throws Exception {
        DataAccessException exception = new DataAccessException();
        doThrow(exception).when(transactions[NR_TRANSACTIONS / 2]).commit();

        try {
            transactionWrapper.commit();
        } catch (DataAccessException e) {
            assertEquals(exception, e);
        }

        for (Transaction t : transactions) {
            verify(t, times(1)).commit();
        }
    }

    @Test
    public void rollbackMultipleTransactions_oneThrowsException_allTransactionsHaveBeenRolledback() throws Exception {
        DataAccessException exception = new DataAccessException();
        doThrow(exception).when(transactions[NR_TRANSACTIONS / 2]).commit();

        try {
            transactionWrapper.rollback();
        } catch (DataAccessException e) {
            assertEquals(exception, e);
        }

        for (Transaction t : transactions) {
            verify(t, times(1)).rollback();
        }
    }

    @Test
    public void closeMultipleTransactions_oneThrowsException_allTransactionsHaveBeenClosed() throws Exception {
        DataAccessException exception = new DataAccessException();
        doThrow(exception).when(transactions[NR_TRANSACTIONS / 2]).commit();

        try {
            transactionWrapper.close();
        } catch (DataAccessException e) {
            assertEquals(exception, e);
        }

        for (Transaction t : transactions) {
            verify(t, times(1)).close();
        }
    }

}