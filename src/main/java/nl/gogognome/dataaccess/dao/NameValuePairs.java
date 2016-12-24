package nl.gogognome.dataaccess.dao;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class NameValuePairs implements Iterable<NameValuePair> {

    private final List<NameValuePair> nameValuePairs = new ArrayList<>();

    public NameValuePairs add(String name, Class<?> type, Object value) {
        nameValuePairs.add(new NameValuePair(name, type, value));
        return this;
    }

    public void replace(String name, Class<?> type, Object value) {
        remove(name);
        add(name, type, value);
    }

    public NameValuePairs add(String name, String value) {
        add(name, String.class, value);
        return this;
    }

    public NameValuePairs add(String name, Enum<?> value) {
        return add(name, String.class, value != null ? value.name() : null);
    }

    public NameValuePairs addEmptyStringToNull(String name, String value) {
        add(name, String.class, value == null || value.isEmpty() ? null : value);
        return this;
    }

    public NameValuePairs addTimestamp(String name, Date value) {
        add(name, Timestamp.class, value != null ? new Timestamp(value.getTime()) : null);
        return this;
    }

    public NameValuePairs add(String name, LocalDateTime localDatetime) {
        Timestamp timestamp = null;
        if (localDatetime != null) {
            Instant instant = localDatetime.atZone(ZoneId.systemDefault()).toInstant();
            timestamp = new Timestamp(instant.toEpochMilli());
        }
        return add(name, Timestamp.class, timestamp);
    }

    public NameValuePairs add(String name, Instant instant) {
        Timestamp timestamp = null;
        if (instant != null) {
            timestamp = new Timestamp(instant.toEpochMilli());
        }
        return add(name, Timestamp.class, timestamp);
    }

    public NameValuePairs add(String name, int value) {
        add(name, Integer.class, value);
        return this;
    }

    public NameValuePairs add(String name, char value) {
        add(name, String.class, Character.toString(value));
        return this;
    }

    public NameValuePairs add(String name, long value) {
        add(name, Long.class, value);
        return this;
    }

    public NameValuePairs add(String name, BigDecimal value) {
        add(name, BigDecimal.class, value);
        return this;
    }

    public NameValuePairs add(String name, double value) {
        add(name, Double.class, value);
        return this;
    }

    public NameValuePairs add(String name, float value) {
        add(name, Float.class, value);
        return this;
    }

    public NameValuePairs add(String name, boolean value) {
        add(name, Boolean.class, value);
        return this;
    }

    public NameValuePairs add(String name, Date value) {
        add(name, Date.class, value);
        return this;
    }

    public NameValuePairs add(String name, Object... values) {
        List<String> strings = new ArrayList<>(values.length);
        for (Object value : values) {
            if (value == null) {
                strings.add(null);
            } else {
                strings.add(value.toString());
            }
        }
        add(name, String.class, strings);
        return this;
    }

    public NameValuePairs addLiteral(String name, String value) {
        add(name, Literal.class, new Literal(value));
        return this;
    }

    /**
     * Removes the value for the specified name.
     *
     * @param name
     *            the name
     */
    public void remove(String name) {
        Iterator<NameValuePair> iter = nameValuePairs.iterator();
        while (iter.hasNext()) {
            NameValuePair nvp = iter.next();
            if (nvp.getName().equals(name)) {
                iter.remove();
            }
        }
    }

    /**
     * Gets the {@link NameValuePair} with the specified name.
     *
     * @param name
     *            the name
     * @return the {@link NameValuePair} or <code>null</code> if no {@link NameValuePair} was specified for the name.
     */
    public NameValuePair getNameValuePair(String name) {
        for (NameValuePair nvp : nameValuePairs) {
            if (nvp.getName().equals(name)) {
                return nvp;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(1000);
        for (int i = 0; i < nameValuePairs.size(); i++) {
            sb.append(nameValuePairs.get(i));
            if (i + 1 < nameValuePairs.size()) {
                sb.append(',');
            }
        }
        return sb.toString();
    }

    @Override
    public Iterator<NameValuePair> iterator() {
        return nameValuePairs.iterator();
    }

    public int size() {
        return nameValuePairs.size();
    }

    public boolean isEmpty() {
        return nameValuePairs.isEmpty();
    }

    /**
     * Gets the value for the specified name.
     *
     * @param name
     *            the name
     * @return the value or null if no value is present for the name.
     */
    public Object getValue(String name) {
        for (NameValuePair nvp : nameValuePairs) {
            if (nvp.getName().equals(name)) {
                return nvp.getValue();
            }
        }
        return null;
    }

    public List<String> getNames() {
        return nameValuePairs.stream().map(NameValuePair::getName).collect(toList());
    }

    /**
     * Gets a new NameValuePairs instance that consists of the subset of name value pairs from this instance specified by names.
     *
     * @param names
     *            the names of the pairs to be copied in the subset
     * @return the subset
     */
    public NameValuePairs getSubset(Collection<String> names) {
        NameValuePairs subset = new NameValuePairs();
        for (NameValuePair nvp : this.nameValuePairs) {
            if (names.contains(nvp.getName())) {
                subset.nameValuePairs.add(nvp);
            }
        }
        return subset;
    }
}