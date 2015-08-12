package nl.gogognome.dataaccess.dao;

import nl.gogognome.dataaccess.transaction.JdbcTransaction;
import nl.gogognome.dataaccess.transaction.CurrentTransaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Base class for Data Access Objects (DAOs). Use this base class in combination with
 * Transactions that implement the JdbcTransaction interface.
 */
public class AbstractDAO {

    private Object[] connectionParameters;

    /**
     * Constructor.
     * @param connectionParameters the parameters passed to the JdbcTransaction.
     */
    protected AbstractDAO(Object... connectionParameters) {
        this.connectionParameters = connectionParameters;
    }

    /**
     * Creates a prepared statement. This method actually returns a {@link PreparedStatementWrapper}, which has some advantages over using a regular
     * {@link PreparedStatement}.
     *
     * @param query
     *            the query
     * @return the prepared statement
     * @throws SQLException
     *             if a problem occurs
     */
    protected PreparedStatementWrapper prepareStatement(String query, Object... parameters) throws SQLException {
        Connection connection = ((JdbcTransaction) CurrentTransaction.get()).getConnection(connectionParameters);
        return PreparedStatementWrapper.preparedStatement(connection, query, parameters);
    }

    /**
     * Creates a record in the database using name value pairs.
     *
     * @param tableName
     *            the name of the table in which the record is to be created
     * @param nameValuePairs
     *            the name value pairs that define the contents of the record
     * @throws SQLException
     *             if a problem occurs
     */
    protected void insert(String tableName, NameValuePairs nameValuePairs) throws SQLException {
        String insertStatement = buildInsertStatement(tableName, nameValuePairs);
        try (PreparedStatementWrapper statement = prepareStatement(insertStatement)) {
            int index = 1;
            for (NameValuePair nvp : nameValuePairs) {
                if (!nvp.getType().equals(Literal.class)) {
                    DAOUtil.setStatementValue(statement, index, nvp.getType(), nvp.getValue());
                    index++;
                }
            }

            statement.executeUpdate();
        }
    }

    private String buildInsertStatement(String tableName, NameValuePairs nameValuePairs) {
        StringBuilder sb = new StringBuilder(1000);
        sb.append("insert into ").append(tableName).append(" (");
        for (Iterator<NameValuePair> iter = nameValuePairs.iterator(); iter.hasNext();) {
            NameValuePair nvp = iter.next();
            sb.append(nvp.getName());
            if (iter.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(") values (");
        for (Iterator<NameValuePair> iter = nameValuePairs.iterator(); iter.hasNext();) {
            NameValuePair nvp = iter.next();
            if (nvp.getType().equals(Literal.class)) {
                sb.append(((Literal) nvp.getValue()).getValue());
            } else {
                sb.append('?');
            }
            if (iter.hasNext()) {
                sb.append(',');
            }
        }
        sb.append(')');
        return sb.toString();
    }

    protected long getNextLongFromSequence(String sequenceName) throws SQLException {
        return execute("select " + sequenceName + ".nextval from DUAL").getFirst(result -> result.getLong(1));
    }

    protected QueryBuilder execute(String sqlStatement, Object... parameters) {
        return new QueryBuilder(connectionParameters).execute(sqlStatement, parameters);
    }

}
