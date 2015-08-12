package nl.gogognome.dataaccess.transaction;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.util.CreationStack;

import java.util.ArrayList;
import java.util.List;

public class CompositeTransaction implements Transaction {

    protected List<Transaction> wrappedTransactions = new ArrayList<>();

    private CreationStack creationStack;

    public CompositeTransaction() {
        if (TransactionSettings.storeCreationStackForTransactions) {
            creationStack = new CreationStack();
        }
    }

    public void addTransaction(Transaction transaction) {
        wrappedTransactions.add(transaction);
    }

    public void commit() throws DataAccessException {
        DataAccessException dataAccessException = null;
        for (Transaction t : wrappedTransactions) {
            try {
                t.commit();
            } catch (DataAccessException e) {
                dataAccessException = e;
            } catch (Exception e) {
                dataAccessException = new DataAccessException("Committing transaction failed: " + e.getMessage(), e);
            }
        }

        if (dataAccessException != null) {
            throw dataAccessException;
        }
    }

    public void rollback() throws DataAccessException {
        DataAccessException dataAccessException = null;
        for (Transaction t : wrappedTransactions) {
            try {
                t.rollback();
            } catch (DataAccessException e) {
                dataAccessException = e;
            } catch (Exception e) {
                dataAccessException = new DataAccessException("Rolling back transaction failed: " + e.getMessage(), e);
            }
        }

        if (dataAccessException != null) {
            throw dataAccessException;
        }
    }

    public void close() throws DataAccessException {
        DataAccessException dataAccessException = null;
        for (Transaction t : wrappedTransactions) {
            try {
                t.close();
            } catch (DataAccessException e) {
                dataAccessException = e;
            } catch (Exception e) {
                dataAccessException = new DataAccessException("Failed closing transaction failed: " + e.getMessage(), e);
            }
        }

        wrappedTransactions.clear();

        if (dataAccessException != null) {
            throw dataAccessException;
        }
    }

    public String getCreationDetails() {
        if (creationStack != null) {
            return creationStack.toString();
        } else {
            return "Creation stacks are not stored.";
        }
    }

}
