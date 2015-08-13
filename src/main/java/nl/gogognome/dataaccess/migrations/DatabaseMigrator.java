package nl.gogognome.dataaccess.migrations;

import nl.gogognome.dataaccess.DataAccessException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class DatabaseMigrator {

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
}
