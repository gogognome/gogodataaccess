package nl.gogognome.dataaccess.dao;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.transaction.CompositeDatasourceTransaction;
import nl.gogognome.dataaccess.transaction.CurrentTransaction;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;

import java.sql.SQLException;

public class BaseInMemTransactionTest {

    private static int uniqueId = 0;

    @Before
    public void createInMemoryDatabase() throws DataAccessException, SQLException {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + getClass().getSimpleName() + "_" + uniqueId + ";MVCC=TRUE");
        uniqueId++;

        CompositeDatasourceTransaction.registerDataSource("test", dataSource);

        CurrentTransaction.create();
    }

    @After
    public void closeTransaction() throws DataAccessException {
        CurrentTransaction.close(false);
    }

}
