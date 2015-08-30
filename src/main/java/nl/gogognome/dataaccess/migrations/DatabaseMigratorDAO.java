package nl.gogognome.dataaccess.migrations;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.dao.AbstractDAO;
import nl.gogognome.dataaccess.dao.NameValuePairs;
import nl.gogognome.dataaccess.transaction.CurrentTransaction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class DatabaseMigratorDAO extends AbstractDAO {

    private Object[] connectionParameters;

    public DatabaseMigratorDAO(Object... connectionParameters) {
        super(connectionParameters);
        this.connectionParameters = connectionParameters;
    }

    public List<Migration> loadMigrationsFromResource(String path) throws IOException, DataAccessException {
        List<Migration> migrations = new ArrayList<>();
        int index = path.lastIndexOf('/');
        String pathToDir = index > 0 ? path.substring(0, index + 1) : "/";

        try (InputStream inputStream = getClass().getResourceAsStream(path)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            int lineNr = 1;
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                Migration migration = parseLine(pathToDir, line, lineNr);
                if (migration != null) {
                    migrations.add(migration);
                }
                lineNr++;
            }
        }

        migrations.sort((m1, m2) -> Long.compare(m1.getId(), m2.getId()));
        validateIdsAreUnique(migrations);

        return migrations;
    }

    private Migration parseLine(String pathToDir, String line, int lineNr) throws DataAccessException {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("//") || line.startsWith("#") || line.startsWith(";")) {
            return null;
        }

        int index = line.indexOf(':');
        if (index < 0) {
            throw new DataAccessException("Syntax error found in line " + lineNr + " \"" + line + "\". Expected syntax: <id>: <migration script|migration class>");
        }

        long id;
        try {
            id = Long.parseLong(line.substring(0, index).trim());
        } catch (NumberFormatException e) {
            throw new DataAccessException("Syntax error found in line " + lineNr + " \"" + line + "\". Id must be a valid long.");
        }

        String value = line.substring(index + 1).trim();
        Migration migration;
        try {
            migration = (Migration) Class.forName(value).getConstructor(long.class).newInstance(id);
        } catch (Exception e) {
            migration = new ResourceMigration(id, value.startsWith("/") ? value : pathToDir + value);
        }

        return migration;
    }

    private void validateIdsAreUnique(List<Migration> migrations) throws DataAccessException {
        Migration previousMigration = null;
        for (Migration migration : migrations) {
            if (previousMigration != null) {
                if (previousMigration.getId() == migration.getId()) {
                    throw new DataAccessException("Multiple migrations have id " + migration.getId() + "! Migrations must have unique ids.");
                }
            }
            previousMigration = migration;
        }
    }

    public List<Long> getMigrationsAppliedToDatabase() throws SQLException {
        execute("create table if not exists _database_migrations (id bigint, timestamp timestamp, primary key(id))").ignoreResult();
        return execute("select id from _database_migrations order by id").toList(r -> r.getLong(1));
    }

    public List<Long> applyMigrations(List<Migration> migrations) throws SQLException, DataAccessException {
        List<Long> appliedMigrations = getMigrationsAppliedToDatabase();
        List<Long> newAppliedMigrations = new ArrayList<>();
        for (Migration migration : migrations) {
            if (!appliedMigrations.contains(migration.getId())) {
                try {
                    migration.applyChanges(connectionParameters);
                    insert("_database_migrations", new NameValuePairs().add("id", migration.getId()).add("timestamp", new Timestamp(System.currentTimeMillis())));
                    CurrentTransaction.get().commit();
                    newAppliedMigrations.add(migration.getId());
                } catch (SQLException | DataAccessException e) {
                    throw e;
                } catch (Exception e) {
                    throw new DataAccessException("Failed to apply migration " + migration.getId() + ": " + e.getMessage(), e);
                }
            }
        }
        return newAppliedMigrations;
    }

    /**
     * Applies all migrations from the specified url.
     *
     * @param path Absolute location of file that enumerates all migrations. The file must be present as resource on the class path.
     * @return the ids of the migrations that have been applied by this method
     * @throws IOException if a problem occurs reading migration file
     * @throws DataAccessException if some problem occurs
     * @throws SQLException if an SQL problem occurs
     */
    public List<Long> applyMigrationsFromResource(String path) throws IOException, DataAccessException, SQLException {
        List<Migration> migrations = loadMigrationsFromResource(path);
        return applyMigrations(migrations);
    }
}