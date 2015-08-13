package nl.gogognome.dataaccess.migrations;

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
    public void applyChanges() {
        // TODO
    }
}
