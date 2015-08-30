package nl.gogognome.dataaccess.migrations;

public class SampleMigration implements Migration {

    private final long id;

    public SampleMigration(long id) {
        this.id = id;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void applyChanges(Object... connectionParameters) throws Exception {
        // does nothing
    }
}
