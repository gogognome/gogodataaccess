package nl.gogognome.dataaccess.migrations;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.dao.AbstractDAO;
import nl.gogognome.dataaccess.dao.NameValuePairs;
import nl.gogognome.dataaccess.transaction.CurrentTransaction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DatabaseMigratorDAO extends AbstractDAO {

    public DatabaseMigratorDAO(Object... connectionParameters) {
        super(connectionParameters);
    }

    public List<Migration> loadMigrationsFromResource(URL url) throws IOException, DataAccessException {
        List<Migration> migrations = new ArrayList<>();

        try (InputStream inputStream = url.openStream()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            int lineNr = 1;
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                Migration migration = parseLine(line, lineNr);
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

    private Migration parseLine(String line, int lineNr) throws DataAccessException {
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
            migration = new ResourceMigration(id, value);
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
                    migration.applyChanges();
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
     * @param url location of file that enumerates all migrations
     * @return the ids of the migrations that have been applied by this method
     * @throws IOException if a problem occurs reading migration file
     * @throws DataAccessException if some problem occurs
     * @throws SQLException if an SQL problem occurs
     */
    public List<Long> applyMigrationsFromResource(URL url) throws IOException, DataAccessException, SQLException {
        List<Migration> migrations = loadMigrationsFromResource(url);
        return applyMigrations(migrations);
    }
}