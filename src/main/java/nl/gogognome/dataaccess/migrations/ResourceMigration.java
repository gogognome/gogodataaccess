package nl.gogognome.dataaccess.migrations;

import nl.gogognome.dataaccess.dao.AbstractDAO;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;

public class ResourceMigration implements Migration {

    private final long id;
    private final String sqlScriptResource;

    public ResourceMigration(long id, String sqlScriptResource) {
        this.id = id;
        this.sqlScriptResource = sqlScriptResource;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void applyChanges(Object... connectionParameters) throws Exception {
        try (Reader reader = new InputStreamReader(getClass().getResourceAsStream(sqlScriptResource))) {
            new RunScriptDAO(connectionParameters).runScript(reader);
        }
    }

    private static class RunScriptDAO extends AbstractDAO {

        RunScriptDAO(Object... connectionParameters) {
            super(connectionParameters);
        }

        void runScript(Reader reader) throws IOException, SQLException {
            super.runScript(reader, false);
        }
    }
}
