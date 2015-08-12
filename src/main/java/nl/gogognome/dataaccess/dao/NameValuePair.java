package nl.gogognome.dataaccess.dao;

public class NameValuePair {
    private final String name;
    private final Object value;
    private final Class<?> type;

    public NameValuePair(String name, Class<?> type, Object value) {
        super();
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return name + '=' + value;
    }
}