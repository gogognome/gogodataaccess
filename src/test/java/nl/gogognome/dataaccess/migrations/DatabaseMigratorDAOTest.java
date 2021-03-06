package nl.gogognome.dataaccess.migrations;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.dao.BaseInMemTransactionTest;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class DatabaseMigratorDAOTest extends BaseInMemTransactionTest {

    private DatabaseMigratorDAO databaseMigratorDAO = new DatabaseMigratorDAO("test");
    private String packageName = ("/" + getClass().getPackage().getName() + "/").replaceAll("[.]", "/");

    @Test
    public void loadEmptyFileShouldReturnEmptyList() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigratorDAO.loadMigrationsFromResource(packageName + "emptyFile.txt");

        assertTrue(migrations.isEmpty());
    }

    @Test
    public void loadFileWithOnlyCommentsShouldReturnEmptyList() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigratorDAO.loadMigrationsFromResource(packageName + "fileWithOnlyComments.txt");

        assertTrue(migrations.isEmpty());
    }

    @Test
    public void loadFileWithValidSqlScriptShouldReturnOneMigration() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigratorDAO.loadMigrationsFromResource(packageName + "fileWithValidSqlScript.txt");

        assertEquals(1, migrations.size());
        assertEquals(123L, migrations.get(0).getId());
        assertEquals(ResourceMigration.class, migrations.get(0).getClass());
    }

    @Test
    public void loadFileWithValidMigrationClassShouldReturnOneMigration() throws IOException, DataAccessException {
        List<Migration> migrations = databaseMigratorDAO.loadMigrationsFromResource(packageName + "fileWithValidMigrationClass.txt");

        assertEquals(1, migrations.size());
        assertEquals(456L, migrations.get(0).getId());
        assertEquals(SampleMigration.class, migrations.get(0).getClass());
    }

    @Test(expected = DataAccessException.class)
    public void loadFileWithInvalidSyntaxShouldFail() throws IOException, DataAccessException {
        databaseMigratorDAO.loadMigrationsFromResource(packageName + "fileWithInvalidSyntax.txt");
    }

    @Test(expected = DataAccessException.class)
    public void loadFileWithInvalidIdSyntaxShouldFail() throws IOException, DataAccessException {
        databaseMigratorDAO.loadMigrationsFromResource(packageName + "fileWithInvalidIdSyntax.txt");
    }

    @Test(expected = DataAccessException.class)
    public void loadFileWithDuplicateIdsShouldFail() throws IOException, DataAccessException {
        databaseMigratorDAO.loadMigrationsFromResource(packageName + "fileWithDuplicateIds.txt");
    }

    @Test
    public void whenTableWithMigrationsDoesNotExistThenAppliedMigrationsMustBeEmptyList() throws SQLException {
        List<Long> migrationIds = databaseMigratorDAO.getMigrationsAppliedToDatabase();
        assertTrue(migrationIds.isEmpty());
    }

    @Test
    public void whenMigrationsArePresentThenAppliedMigrationsMustContainAllMigrationIds() throws SQLException, DataAccessException {
        databaseMigratorDAO.applyMigrations(asList(new DummyMigration(1), new DummyMigration(2), new DummyMigration(3)));

        List<Long> migrationIds = databaseMigratorDAO.getMigrationsAppliedToDatabase();
        assertEquals(asList(1L, 2L, 3L), migrationIds);
    }

    @Test
    public void applyMigrationsOnlyAppliesMigrationsThatNeedToBeApplied() throws SQLException, DataAccessException {
        databaseMigratorDAO.applyMigrations(asList(new DummyMigration(1), new DummyMigration(2)));

        DummyMigration migration1 = new DummyMigration(1);
        DummyMigration migration2 = new DummyMigration(2);
        DummyMigration migration3 = new DummyMigration(3);
        DummyMigration migration4 = new DummyMigration(4);
        databaseMigratorDAO.applyMigrations(asList(migration1, migration2, migration3, migration4));

        assertFalse(migration1.isApplied());
        assertFalse(migration2.isApplied());
        assertTrue(migration3.isApplied());
        assertTrue(migration4.isApplied());
    }

    @Test
    public void applyMigrationsShouldCommitAfterEachMigration() throws SQLException, DataAccessException {
        Migration failingMigration = new DummyMigration(2) {
            @Override
            public void applyChanges(Object... connectionParameters) throws Exception {
                throw new Exception("A problem occurred");
            }
        };

        try {
            databaseMigratorDAO.applyMigrations(asList(new DummyMigration(1), failingMigration,  new DummyMigration(3)));
            fail("Expected exception was not thrown");
        } catch (SQLException | DataAccessException e) {
            // ignore expected exception
        }

        assertEquals(singletonList(1L), databaseMigratorDAO.getMigrationsAppliedToDatabase());
    }

    private static class DummyMigration implements Migration {

        private final long id;
        private boolean applied;

        public DummyMigration(long id) {
            this.id = id;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public void applyChanges(Object... connectionParameters) throws Exception {
            applied = true;
        }

        public boolean isApplied() {
            return applied;
        }
    }
}