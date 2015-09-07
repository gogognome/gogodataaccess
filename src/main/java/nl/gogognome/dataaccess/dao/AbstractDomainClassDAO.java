package nl.gogognome.dataaccess.dao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for Data Access Objects (DAOs) for a single type of domain class.
 * Use this base class in combination with Transactions that implement the JdbcTransaction interface.
 *
 * @param <D> the type of the domain class
 */
public abstract class AbstractDomainClassDAO<D> extends AbstractDAO {

    protected final String tableName;
    protected final String sequenceName;

    private List<String> cachedPkColumns;

    private String allColumnNames = "*";

    protected AbstractDomainClassDAO(String tableName, Object... connectionParameters) {
        this(tableName, null, connectionParameters);
    }

    protected AbstractDomainClassDAO(String tableName, String sequenceName, Object... connectionParameters) {
        super(connectionParameters);
        this.tableName = tableName;
        this.sequenceName = sequenceName;
    }

    public void setAllColumnNames(String allColumnNames) {
        this.allColumnNames = allColumnNames;
    }

    protected String getSelectClause() {
        return "SELECT " + allColumnNames + " FROM " + tableName + ' ';
    }

    /**
     * Gets all domain objects stored in the database.
     *
     * @return all domain objects
     * @throws SQLException
     *             if a problem occurs
     */
    public List<D> findAll() throws SQLException {
        return findAll((String) null);
    }

    /**
     * Gets all domain objects stored in the database sorted.
     *
     * @param sortClause
     *            if not null, then this will be the column name used to sort the domain objects
     * @return all domain objects
     * @throws SQLException
     *             if a problem occurs
     */
    public List<D> findAll(String sortClause) throws SQLException {
        List<D> list = new ArrayList<>(100);
        String queryString = getSelectClause();
        if (sortClause != null) {
            queryString += " order by " + sortClause;
        }
        try (PreparedStatementWrapper statement = prepareStatement(queryString)) {
            ResultSetWrapper result = statement.executeQuery();
            while (result.next()) {
                list.add(getObjectFromResultSet(result));
            }
        }
        return list;
    }

    /**
     * Checks whether a domain object exists in the database
     *
     * @param id
     *            the ID of the domain object
     * @return <code>true</code> if the domain object exists; <code>false</code> otherwise
     * @throws SQLException
     *             if a problem occurs
     */
    public boolean exists(Object id) throws SQLException {
        return exists(buildIdNameValuePairs(id));
    }

    /**
     * Checks whether a domain object exists in the database
     *
     * @param nameValuePairs
     *            the {@link NameValuePairs} of the primary key
     * @return <code>true</code> if the domain object exists; <code>false</code> otherwise
     * @throws SQLException
     *             if a problem occurs
     */
    public boolean exists(NameValuePairs nameValuePairs) throws SQLException {
        boolean exists;

        StringBuilder sb = new StringBuilder(100);
        sb.append(getSelectClause()).append(" where ");
        appendColumns(sb, getCachedPkColumns(), nameValuePairs);
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            setColumnValues(statement, 1, getCachedPkColumns(), nameValuePairs);
            ResultSetWrapper result = statement.executeQuery();
            exists = result.next(); // if there is no next entry, result.next() returns false;
            result.close();
        }
        return exists;
    }

    /**
     * Checks whether at least one domain objects exists in the database matching the name value pairs.
     *
     * @param nameValuePairs
     *            the {@link NameValuePairs} of the primary key
     * @return true if at least one domain object exists; false otherwise
     * @throws SQLException
     *             if a problem occurs
     */
    public boolean existsAtLeastOne(NameValuePairs nameValuePairs) throws SQLException {
        boolean exists;

        StringBuilder sb = new StringBuilder(100);
        sb.append(getSelectClause());
        DAOUtil.appendWhereClause(sb, nameValuePairs);
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            DAOUtil.setWhereClauseValues(statement, nameValuePairs, 1);
            ResultSetWrapper result = statement.executeQuery();
            exists = result.next(); // if there is no next entry, result.next() returns false;
            result.close();
        }
        return exists;
    }

    public boolean hasAny() throws SQLException {
        return execute("select count(1) from " + tableName).getFirst(r -> r.getInt(1) > 0);
    }

    /**
     * Gets a domain object from the database.
     *
     * @param id
     *            the ID of the domain object
     * @return the domain object or <code>null</code> if the domain object does not exist
     * @throws SQLException
     *             if a problem occurs
     */
    public D find(Object id) throws SQLException {
        return find(buildIdNameValuePairs(id));
    }

    /**
     * Gets a domain object from the database.
     *
     * @param id
     *            the ID of the domain object
     * @return the domain object
     * @throws SQLException
     *             if the domain object does not exist or if a problem occurs
     */
    public D get(Object id) throws SQLException {
        D object = find(id);
        if (object == null) {
            throw new NoRecordFoundException("Table " + tableName + " has no record with id " + id);
        }
        return object;
    }

    /**
     * Gets a domain object from the database.
     *
     * @param nameValuePairs
     *            the {@link NameValuePairs} of the primary key
     * @return the domain object or <code>null</code> if the domain object does not exist
     * @throws SQLException
     *             if a problem occurs
     */
    protected D find(NameValuePairs nameValuePairs) throws SQLException {
        D object = null;

        StringBuilder sb = new StringBuilder(100);
        sb.append(getSelectClause()).append(" where ");
        appendColumns(sb, getCachedPkColumns(), nameValuePairs);
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            setColumnValues(statement, 1, getCachedPkColumns(), nameValuePairs);
            ResultSetWrapper result = statement.executeQuery();
            if (result.next()) {
                object = getObjectFromResultSet(result);
            }
        }

        return object;
    }

    /**
     * Gets a list of domain objects from the database.
     *
     * @param whereClause
     *            the name value pairs used to create a where clause
     * @return the list of domain objects that match all name value pairs
     * @throws SQLException
     *             if a problem occurs
     */
    public List<D> findAll(NameValuePairs whereClause) throws SQLException {
        return findAll(whereClause, null);
    }

    /**
     * Gets a list of domain objects from the database.
     *
     * @param nameValuePairs
     *            the name value pairs used to create a where clause
     * @param sortClause
     *            if not null, then this will be the column name used to sort the domain objects
     * @return the list of domain objects that match all name value pairs
     * @throws SQLException
     *             if a problem occurs
     */
    public List<D> findAll(NameValuePairs nameValuePairs, String sortClause) throws SQLException {
        if (nameValuePairs.isEmpty())
            return findAll(sortClause);

        List<D> list = new ArrayList<>(100);
        StringBuilder sb = new StringBuilder(200);
        sb.append(getSelectClause());
        DAOUtil.appendWhereClause(sb, nameValuePairs);

        if (sortClause != null) {
            sb.append(" order by ").append(sortClause);
        }
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            DAOUtil.setWhereClauseValues(statement, nameValuePairs, 1);
            ResultSetWrapper result = statement.executeQuery();
            while (result.next()) {
                list.add(getObjectFromResultSet(result));
            }
        }
        return list;
    }

    /**
     * Returns the first domain object that matches the specified where clause
     *
     * @param nameValuePairs
     *            defines the where clause
     * @return the first domain object or <code>null</code> if no domain object was found
     * @throws SQLException
     */
    public D first(NameValuePairs nameValuePairs) throws SQLException {
        D object = null;
        StringBuilder sb = new StringBuilder(200);
        sb.append(getSelectClause());
        DAOUtil.appendWhereClause(sb, nameValuePairs);
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            DAOUtil.setWhereClauseValues(statement, nameValuePairs, 1);
            ResultSetWrapper result = statement.executeQuery();
            if (result.next()) {
                object = getObjectFromResultSet(result);
            }
        }
        return object;
    }

    public List<D> findAllWhere(String whereclause) throws SQLException {
        List<D> objects = new ArrayList<>(100);
        try (PreparedStatementWrapper statement = prepareStatement("SELECT * FROM " + tableName + " WHERE " + whereclause)) {
            ResultSetWrapper result = statement.executeQuery();
            while (result.next()) {
                objects.add(getObjectFromResultSet(result));
            }
        }
        return objects;
    }

    /**
     * Deletes a list of domain objects from the database.
     *
     * @param nameValuePairs
     *            the name value pairs used to create a where clause
     * @return the number of records deleted
     * @throws SQLException
     *             if a problem occurs
     */
    public int deleteWhere(NameValuePairs nameValuePairs) throws SQLException {
        StringBuilder sb = new StringBuilder(200);
        sb.append("delete from ").append(tableName);
        DAOUtil.appendWhereClause(sb, nameValuePairs);
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            DAOUtil.setWhereClauseValues(statement, nameValuePairs, 1);
            return statement.executeUpdate();
        }
    }

    /**
     * Deletes a domain object from the database. If the domain object does not exist a NoRecordFoundException is thrown.
     *
     * @param id
     *            the id of the domain object
     * @throws SQLException if a problem occurs
     * @throws NoRecordFoundException if the object to be deleted does not exist
     */
    public void delete(Object id) throws SQLException {
        delete(buildIdNameValuePairs(id));
    }

    /**
     * Deletes a domain object from the database. If the domain object does not exist a NoRecordFoundException is thrown.
     *
     * @param nameValuePairs
     *            the {@link NameValuePairs} of the primary key
     * @throws SQLException if a problem occurs
     * @throws NoRecordFoundException if the object to be deleted does not exist
     */
    protected void delete(NameValuePairs nameValuePairs) throws SQLException {
        StringBuilder sb = new StringBuilder(100);
        sb.append("delete from ").append(tableName).append(" where ");
        appendColumns(sb, getCachedPkColumns(), nameValuePairs);
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            setColumnValues(statement, 1, getCachedPkColumns(), nameValuePairs);
            int nrOfRowsDeleted = statement.executeUpdate();
            if (nrOfRowsDeleted != 1) {
                throw new NoRecordFoundException("Table " + tableName + "has no record with id " + nameValuePairs);
            }
        }
    }

    /**
     * Creates a domain object in the database. The value for the primary key does not have to be filled in.
     * The returned object will have the primary key filled in.
     * @param object
     *            the domain object to be created in the database
     * @return a new instance of the domain object with the values as stored in the database.
     * @throws SQLException
     */
    public D create(D object) throws SQLException {
        NameValuePairs nvp = createAndReturnNameValuePairs(object);
        return getObjectFromResultSet(convertNameValuePairsToResultSet(nvp));
    }

    /**
     * Creates a domain object in the database
     *
     * @param object
     *            the domain object to be created in the database
     * @return the name value pairs used to create the domain object
     * @throws SQLException
     *             if a problem occurs
     */
    protected NameValuePairs createAndReturnNameValuePairs(D object) throws SQLException {
        NameValuePairs nameValuePairs = getNameValuePairs(object);
        addAutoGeneratedValues(nameValuePairs, object);
        insert(tableName, nameValuePairs);
        return nameValuePairs;
    }

    protected ResultSetWrapper convertNameValuePairsToResultSet(NameValuePairs nameValuePairs) {
        return new ResultSetWrapper(new NameValuePairsResultSet(nameValuePairs));
    }

    /**
     * If a sequence name is set then the value for the primary key is generated using that sequence.
     *
     * @param nameValuePairs
     *            the name value pairs
     * @param object
     *            the domain object
     * @throws SQLException
     */
    protected void addAutoGeneratedValues(NameValuePairs nameValuePairs, D object) throws SQLException {
        if (sequenceName != null) {
            if (getCachedPkColumns().size() != 1) {
                throw new SQLException("Number of primary key columns should be 1 but was " + getCachedPkColumns().size());
            }
            String pkColumn = getCachedPkColumns().get(0);
            nameValuePairs.remove(pkColumn);
            nameValuePairs.add(pkColumn, getNextLongFromSequence(sequenceName));
        }
    }

    /**
     * Updates an existing domain object in the database.
     *
     * @param object
     *            the domain object
     * @throws SQLException if a problem occurs
     * @throws NoRecordFoundException if the object to be deleted does not exist
     */
    public void update(D object) throws SQLException {
        NameValuePairs nameValuePairs = getNameValuePairs(object);
        NameValuePairs whereClause = nameValuePairs.getSubset(getCachedPkColumns());

        int nrRowsUpdated = updateWhere(nameValuePairs, whereClause);

        if (nrRowsUpdated != 1) {
            throw new NoRecordFoundException("Table " + tableName + " has no record with id " + nameValuePairs);
        }
    }

    /**
     * Updates records selected by the where clause
     *
     * @param nameValuePairs
     *            the name value pairs used to set the new values of the selected records
     * @param whereClause
     *            where clause specified by name value pairs
     * @return the number of records that have been updated
     * @throws SQLException
     */
    protected int updateWhere(NameValuePairs nameValuePairs, NameValuePairs whereClause) throws SQLException {
        StringBuilder sb = new StringBuilder(1000);
        sb.append("update ").append(tableName).append(" set ");

        List<String> setClauses = new ArrayList<>();
        for (NameValuePair nvp : nameValuePairs) {
            if (!getCachedPkColumns().contains(nvp.getName()))
                setClauses.add(nvp.getName() + "=?");
        }
        sb.append(String.join(", ", setClauses));

        sb.append(" where ");
        appendColumns(sb, whereClause.getNames(), whereClause);
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            int index = 1;
            for (NameValuePair nvp : nameValuePairs) {
                if (!getCachedPkColumns().contains(nvp.getName())) {
                    DAOUtil.setStatementValue(statement, index, nvp.getType(), nvp.getValue());
                    index++;
                }
            }

            setColumnValues(statement, index, whereClause.getNames(), whereClause);

            return statement.executeUpdate();
        }

    }

    /**
     * Gets the number of rows in the table that match the specified name value pairs.
     *
     * @param nameValuePairs
     *            the name value pairs used to create a where clause. <code>null</code> indicates that all rows of the table must be counted
     * @return the number of rows
     * @throws SQLException
     *             if a problem occurs
     */
    public int count(NameValuePairs nameValuePairs) throws SQLException {
        StringBuilder sb = new StringBuilder(1000);
        sb.append("select count(*) from ").append(tableName);
        if (nameValuePairs != null) {
            DAOUtil.appendWhereClause(sb, nameValuePairs);
        }
        try (PreparedStatementWrapper statement = prepareStatement(sb.toString())) {
            if (nameValuePairs != null) {
                DAOUtil.setWhereClauseValues(statement, nameValuePairs, 1);
            }

            ResultSetWrapper result = statement.executeQuery();
            if (result.next()) {
                return result.getInt(1);
            } else {
                throw new SQLException("Could not count the number of rows in " + tableName + "!");
            }
        }
    }

    /**
     * Converts an id object to a NameValuePairs instance. Override this method if the default implementation
     * does not fit your requirements.
     *
     * @param id id to be converted
     * @return a NameValuePairs.
     */
    protected NameValuePairs buildIdNameValuePairs(Object id) throws SQLException {
        if (id instanceof Integer) {
            return new NameValuePairs().add(getPkColumn(), (Integer) id);
        }
        if (id instanceof Long) {
            return new NameValuePairs().add(getPkColumn(), (Long) id);
        }
        if (id instanceof String) {
            return new NameValuePairs().add(getPkColumn(), (String) id);
        }
        if (id == null) {
            throw new NullPointerException("id must not be null");
        }
        throw new IllegalArgumentException("unsupported class " + id.getClass() + " for id. Override this method in your DAO to fix this problem.");
    }

    /**
     * Gets a domain object from a result set. Do not call <code>result.next()</code>.
     *
     * @param result
     *            the result set
     * @return the domain object
     * @throws SQLException
     *             if a problem occurs
     */
    protected abstract D getObjectFromResultSet(ResultSetWrapper result) throws SQLException;

    /**
     * Gets name value pairs that represent the domain object. The names correspond to columns in the database table.
     *
     * @param domainObject
     *            the domain object
     * @return the collection of name value pairs
     * @throws SQLException
     *             if a problem occurs
     */
    protected abstract NameValuePairs getNameValuePairs(D domainObject) throws SQLException;

    /**
     * Gets the column that contains the primary key for the domain objects.
     * Override this method if a table has a primary key consisting of one column and that differs from "id".
     *
     * @return the column of the primary key
     */
    protected String getPkColumn() {
        return "id";
    }

    /**
     * Gets the columns of the primary key. Override this method if a table has a primary key consisting of more than one column.
     *
     * @return the columns of the primary key
     */
    protected List<String> getPkColumns() {
        return Collections.singletonList(getPkColumn());
    }

    /**
     * Does the same as {@link #getPkColumns()}, but stores the result in {@link #cachedPkColumns}. Once {@link #cachedPkColumns} has been set then subsequent
     * calls to this method return the value of {@link #cachedPkColumns}.
     *
     * <p>
     * By caching the list of columns values we prevent creating many instances of lists that are used very shortly.
     *
     * @return the columns of the primary key
     */
    private List<String> getCachedPkColumns() {
        if (cachedPkColumns == null) {
            cachedPkColumns = getPkColumns();
        }
        return cachedPkColumns;
    }

    private void appendColumns(StringBuilder sb, Iterable<String> columnNames, NameValuePairs nameValuePairs) {
        boolean first = true;
        for (String columnName : columnNames) {
            if (!first) {
                sb.append(" and ");
            }
            sb.append(columnName);
            if (nameValuePairs.getNameValuePair(columnName).getValue() != null) {
                sb.append("=?");
            } else {
                sb.append(" is null");
            }
            first = false;
        }
    }

    /**
     * Sets column values for the specified columns in a prepared statement.
     *
     * @param statement
     *            the prepared statement
     * @param index
     *            the index of the first pk column
     * @param columnNames
     *            the names of the columns
     * @param nameValuePairs
     *            the {@link NameValuePairs} containing the values for the PK columns
     * @throws SQLException
     *             if a problem occurs
     */
    private void setColumnValues(PreparedStatementWrapper statement, int index, Iterable<String> columnNames, NameValuePairs nameValuePairs) throws SQLException {
        for (String columnName : columnNames) {
            NameValuePair nvp = nameValuePairs.getNameValuePair(columnName);
            if (nvp.getValue() != null) {
                DAOUtil.setStatementValue(statement, index, nvp.getType(), nvp.getValue());
                index++;
            }
        }
    }

}
