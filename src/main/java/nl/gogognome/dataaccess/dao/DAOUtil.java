package nl.gogognome.dataaccess.dao;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Iterator;

public class DAOUtil {
    public static void appendWhereClause(StringBuilder sb, NameValuePairs nameValuePairs) {
        sb.append(" where ");
        for (Iterator<NameValuePair> iter = nameValuePairs.iterator(); iter.hasNext();) {
            NameValuePair nvp = iter.next();
            sb.append(nvp.getName());
            if (nvp.getValue() == null) {
                sb.append(" is null");
            } else {
                sb.append("=?");
            }
            if (iter.hasNext()) {
                sb.append(" and ");
            }
        }
    }

    public static <T> String convertToPivotClause(Iterable<T> items) throws SQLException {
        int nrItems = 0;
        StringBuilder sb = new StringBuilder(1000);
        sb.append('(');
        for (T item : items) {
            if (sb.length() != 1) {
                sb.append(',');
            }
            sb.append('\'').append(item).append('\'');

            nrItems += 1;
            if (nrItems == 1001) {
                throw new SQLException("Cannot put more than 1000 elements in a pivot for Oracle databases.");
            }
        }

        if (nrItems == 0) {
            throw new SQLException("Cannot put zero elements in a pivot for Oracle databases.");
        }
        sb.append(')');
        return sb.toString();
    }

    public static void setWhereClauseValues(PreparedStatementWrapper statement, NameValuePairs nameValuePairs, int index) throws SQLException {
        for (NameValuePair nvp : nameValuePairs) {
            if (nvp.getValue() != null) {
                if (!nvp.getType().equals(Literal.class)) {
                    setStatementValue(statement, index, nvp.getType(), nvp.getValue());
                    index++;
                }
            }
        }
    }

    /**
     * Sets a value in a prepared statement.
     *
     * @param statement
     *            the prepared statement
     * @param index
     *            the index
     * @param value
     *            the value
     * @throws SQLException
     *             if a problem occurs
     */
    public static void setStatementValue(PreparedStatementWrapper statement, int index, Class<?> type, Object value) throws SQLException {
        if (value == null) {
            int sqlType;
            if (type.equals(String.class)) {
                sqlType = Types.VARCHAR;
            } else if (type.equals(Timestamp.class)) {
                sqlType = Types.TIMESTAMP;
            } else if (type.equals(BigDecimal.class)) {
                sqlType = Types.NUMERIC;
            } else if (type.equals(Double.class)) {
                sqlType = Types.NUMERIC;
            } else if (type.equals(Integer.class)) {
                sqlType = Types.NUMERIC;
            } else if (type.equals(Long.class)) {
                sqlType = Types.NUMERIC;
            } else if (type.equals(Boolean.class)) {
                sqlType = Types.NUMERIC;
            } else if (type.equals(Date.class)) {
                sqlType = Types.TIMESTAMP;
            } else {
                throw new SQLException("Value of type " + type + " is not supported.");
            }
            statement.setNull(index, sqlType);
        } else {
            if (type.equals(String.class)) {
                statement.setString(index, (String) value);
            } else if (type.equals(Timestamp.class)) {
                statement.setTimestamp(index, (Timestamp) value);
            } else if (type.equals(BigDecimal.class)) {
                statement.setBigDecimal(index, (BigDecimal) value);
            } else if (type.equals(Double.class)) {
                statement.setDouble(index, (Double) value);
            } else if (type.equals(Float.class)) {
                statement.setFloat(index, (Float) value);
            } else if (type.equals(Integer.class)) {
                statement.setInt(index, (Integer) value);
            } else if (type.equals(Long.class)) {
                statement.setLong(index, (Long) value);
            } else if (type.equals(Boolean.class)) {
                statement.setInt(index, (Boolean) value ? 1 : 0);
            } else if (type.equals(Date.class)) {
                statement.setDate(index, new java.sql.Date(((Date) value).getTime()));
            } else {
                throw new SQLException("Value of type " + type + " is not supported.");
            }
        }
    }
}
