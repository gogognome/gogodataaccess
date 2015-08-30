package nl.gogognome.dataaccess.migrations;

public interface Migration {

    long getId();

    void applyChanges(Object[] connectionParameters) throws Exception;
}
