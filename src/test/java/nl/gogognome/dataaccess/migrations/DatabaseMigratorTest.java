package nl.gogognome.dataaccess.migrations;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class DatabaseMigratorTest {

    private DatabaseMigrator databaseMigrator = new DatabaseMigrator();

    @Test
    public void loadEmptyFileShouldReturnEmptyList() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigrator.loadMigrationsFromResource(getClass().getResource("emptyFile.txt"));

        assertTrue(migrations.isEmpty());
    }

    @Test
    public void loadFileWithOnlyCommentsShouldReturnEmptyList() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigrator.loadMigrationsFromResource(getClass().getResource("fileWithOnlyComments.txt"));

        assertTrue(migrations.isEmpty());
    }

    @Test
    public void loadFileWithValidSqlScriptShouldReturnOneMigration() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigrator.loadMigrationsFromResource(getClass().getResource("fileWithValidSqlScript.txt"));

        assertEquals(1, migrations.size());
        assertEquals(123L, migrations.get(0).getId());
        assertEquals(ResourceMigration.class, migrations.get(0).getClass());
    }

    @Test
    public void loadFileWithValidMigrationClassShouldReturnOneMigration() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigrator.loadMigrationsFromResource(getClass().getResource("fileWithValidMigrationClass.txt"));

        assertEquals(1, migrations.size());
        assertEquals(456L, migrations.get(0).getId());
        assertEquals(SampleMigration.class, migrations.get(0).getClass());
    }

    @Test(expected = DataAccessException.class)
    public void loadFileWithInvalidSyntaxShouldFail() throws IOException, DataAccessException {
        databaseMigrator.loadMigrationsFromResource(getClass().getResource("fileWithInvalidSyntax.txt"));
    }

    @Test(expected = DataAccessException.class)
    public void loadFileWithInvalidIdSyntaxShouldFail() throws IOException, DataAccessException {
        databaseMigrator.loadMigrationsFromResource(getClass().getResource("fileWithInvalidIdSyntax.txt"));
    }

    @Test(expected = DataAccessException.class)
    public void loadFileWithDuplicateIdsShouldFail() throws IOException, DataAccessException {
        databaseMigrator.loadMigrationsFromResource(getClass().getResource("fileWithDuplicateIds.txt"));
    }


}