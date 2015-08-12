package nl.gogognome.dataaccess.dao;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class AbstractDAOTest extends BaseInMemTransactionTest {

    private TestDAO testDAO;

    @Before
    public void initTable() throws DataAccessException, SQLException {
        testDAO = new TestDAO();
        testDAO.createTable();
    }

    @Test
    public void whenValuesAreInsertedTheyCanBeFetchedAgain() throws DataAccessException, SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        assertEquals("one", testDAO.findName(1));
        assertEquals("two", testDAO.findName(2));
        assertEquals("three", testDAO.findName(3));
    }

    @Test
    public void getIntSequenceShouldGetNextValueOfSequence() throws DataAccessException, SQLException {
        testDAO.createSequence();

        assertEquals(1, testDAO.getNextLongFromSequence("test_sequence"));
        assertEquals(2, testDAO.getNextLongFromSequence("test_sequence"));
        assertEquals(3, testDAO.getNextLongFromSequence("test_sequence"));
    }

    @Test(expected = NoRecordFoundException.class)
    public void whenExecuteAndGetFirstFindsNoResultThenItShouldThrowAnException() throws SQLException {
        testDAO.execute("select name from test where id=?", 1).getFirst(r -> r.getString(1));
    }

    @Test
    public void whenExecuteAndGetFirstFindsOneResultsThenItShouldReturnIt() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        assertEquals("two", testDAO.execute("select name from test where id=?", 2).getFirst(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndGetFirstFindsMultipleResultsThenItShouldReturnFirstMatch() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(1, "two");
        testDAO.insert(1, "three");

        String actualName = testDAO.execute("select name from test where id=?", 1).getFirst(r -> r.getString(1));
        assertTrue(asList("one", "two", "three").contains(actualName));
    }

    @Test
    public void whenExecuteAndGetListOfFindsNoResultsThenItShouldReturnAnEmptyList() throws SQLException {
        assertEquals(emptyList(), testDAO.execute("select name from test").toList(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndGetListOfFindsMultipleResultsThenItShouldReturnAllOfThem() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        assertEquals(asList("one", "two", "three"), testDAO.execute("select name from test").toList(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndGetListOfWithPivotFindsNoResultsThenItShouldReturnAnEmptyList() throws SQLException {
        assertEquals(emptyList(), testDAO.execute("select name from test where id in (?)", asList(1, 2, 3)).toList(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndGetListOfWithPivotFindsMultipleResultsThenItShouldReturnAllOfThem() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        assertEquals(asList("one", "two", "three"), testDAO.execute("select name from test where id in (?)", asList(1, 2, 3)).toList( r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndGetSetOfFindsNoResultsThenItShouldReturnAnEmptySet() throws SQLException {
        assertEquals(emptySet(), testDAO.execute("select name from test").toSet(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndGetSetOfFindsMultipleResultsThenItShouldReturnAllOfThem() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        assertEquals(new HashSet<>(asList("one", "two", "three")), testDAO.execute("select name from test").toSet(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndGetHashMapOfWithPivotFindsNoResultsThenItShouldReturnAnEmptyMap() throws SQLException {
        Map<Integer, String> actualMap = testDAO.execute("select id, name from test where id in (?)", asList(1, 2, 3)).toHashMap(
                r -> r.getInt(1),
                r -> r.getString(2));
        assertTrue(actualMap instanceof HashMap);
        assertEquals(emptyMap(), actualMap);
    }

    @Test
    public void whenExecuteAndGetHashMapOfWithPivotFindsMultipleResultsThenItShouldReturnAllOfThem() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        Map<Integer, String> actualMap = testDAO.execute("select id, name from test where id in (?)", asList(1, 2, 3)).toHashMap(
                r -> r.getInt(1),
                r -> r.getString(2));
        assertTrue(actualMap instanceof HashMap);
        assertEquals(new HashMap() {{ put(1, "one"); put(2, "two"); put(3, "three"); }}, actualMap);
    }

    @Test
    public void whenExecuteAndGetHashMapOfFindsNoResultsThenItShouldReturnAnEmptyMap() throws SQLException {
        Map<Integer, String> actualMap = testDAO.execute("select id, name from test").toHashMap(r -> r.getInt(1), r -> r.getString(2));
        assertTrue(actualMap instanceof HashMap);
        assertEquals(emptyMap(), actualMap);
    }

    @Test
    public void whenExecuteAndGetHashMapOfFindsMultipleResultsThenItShouldReturnAllOfThem() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        Map<Integer, String> actualMap = testDAO.execute("select id, name from test").toHashMap(r -> r.getInt(1), r -> r.getString(2));
        assertTrue(actualMap instanceof HashMap);
        assertEquals(new HashMap() {{ put(1, "one"); put(2, "two"); put(3, "three"); }}, actualMap);
    }

    @Test
    public void whenExecuteAndGetTreeMapOfFindsNoResultsThenItShouldReturnAnEmptyMap() throws SQLException {
        Map<Integer, String> actualMap = testDAO.execute("select id, name from test").toTreeMap(r -> r.getInt(1), r -> r.getString(2));
        assertTrue(actualMap instanceof TreeMap);
        assertEquals(emptyMap(), actualMap);
    }

    @Test
    public void whenExecuteAndGetTreeMapOfFindsMultipleResultsThenItShouldReturnAllOfThem() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        Map<Integer, String> actualMap = testDAO.execute("select id, name from test").toTreeMap(r -> r.getInt(1), r -> r.getString(2));
        assertTrue(actualMap instanceof TreeMap);
        assertEquals(new HashMap() {{ put(1, "one"); put(2, "two"); put(3, "three"); }}, actualMap);
    }

    @Test
    public void whenExecuteAndGetMapWithListOfWithPivotFindsNoResultsThenItShouldReturnAnEmptyMap() throws SQLException {
        Map<String, List<Integer>> actualMap = testDAO.execute("select name, id from test where name in (?)", asList("A", "B"))
                .addToMapOfLists(new HashMap<>(), r -> r.getString(1), r -> r.getInt(2));
        assertEquals(emptyMap(), actualMap);
    }

    @Test
    public void whenExecuteAndGetMapWithListOfWithPivotFindsMultipleResultsThenItShouldReturnAllOfThem() throws SQLException {
        testDAO.insert(1, "A");
        testDAO.insert(2, "A");
        testDAO.insert(3, "B");
        testDAO.insert(4, "B");
        testDAO.insert(5, "B");

        Map<String, List<Integer>> actualMap = testDAO.execute("select name, id from test where name in (?)", asList("A", "B"))
                .addToMapOfLists(new HashMap<>(), r -> r.getString(1), r -> r.getInt(2));
        assertEquals(new HashMap() {{ put("A", asList(1, 2)); put("B", asList(3, 4, 5)); }}, actualMap);
    }

    @Test
    public void testExecuteAndCheckIfRecordExists() throws SQLException {
        assertFalse(testDAO.execute("select * from test").exists());

        testDAO.insert(1, "A");
        assertTrue(testDAO.execute("select * from test").exists());
    }

    @Test
    public void whenExecuteForEachIsCalledAndNoRecordMatchesThenTheConsumerIsNotCalled() throws SQLException {
        ResultSetConsumer consumerMock = mock(ResultSetConsumer.class);
        testDAO.execute("select * from test").forEach(consumerMock);
        verify(consumerMock, never()).consume(any(ResultSetWrapper.class));
    }

    @Test
    public void whenExecuteForEachIsCalledAndMultipleRecordMatchThenTheConsumerIsCalledForEachRecordOne() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        ResultSetConsumer consumerMock = mock(ResultSetConsumer.class);
        testDAO.execute("select * from test").forEach(consumerMock);
        verify(consumerMock, times(3)).consume(any(ResultSetWrapper.class));
    }

    @Test
    public void whenExecuteAndFindFirstFindsNoResultThenItShouldThrowAnException() throws SQLException {
        assertNull(testDAO.execute("select name from test where id=?", 1).findFirst(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndFindFirstFindsOneResultsThenItShouldReturnIt() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(2, "two");
        testDAO.insert(3, "three");

        assertEquals("two", testDAO.execute("select name from test where id=?", 2).findFirst(r -> r.getString(1)));
    }

    @Test
    public void whenExecuteAndFindFirstFindsMultipleResultsThenItShouldReturnFirstMatch() throws SQLException {
        testDAO.insert(1, "one");
        testDAO.insert(1, "two");
        testDAO.insert(1, "three");

        assertTrue(asList("one", "two", "three").contains(testDAO.execute("select name from test where id=?", 1).findFirst(r -> r.getString(1))));
    }

    @Test
    public void testUpdateWithPivot() throws SQLException {
        testDAO.insert(1, "aaa");
        testDAO.insert(2, "bbb");
        testDAO.insert(3, "ccc");

        testDAO.execute("update test set name='xxx' where id in(?)", asList(1, 2, 3)).ignoreResult();

        assertEquals(asList("xxx", "xxx", "xxx"), testDAO.execute("select name from test").toList(r -> r.getString(1)));
    }

    private class TestDAO extends AbstractDAO {

        public TestDAO() throws DataAccessException {
            super("test");
        }

        public void createTable() throws SQLException {
            try (PreparedStatementWrapper statement = prepareStatement("create table test (id number, name varchar2(100))")) {
                statement.execute();
            }
        }

        public void createSequence() throws SQLException {
            try (PreparedStatementWrapper statement = prepareStatement("create sequence test_sequence start with 1")) {
                statement.execute();
            }
        }

        public void insert(int id, String name) throws SQLException {
            insert("test", new NameValuePairs().add("id", id).add("name", name));
        }

        public String findName(int id) throws SQLException {
            try (PreparedStatementWrapper statement = prepareStatement("select name from test where id=?", id)) {
                ResultSetWrapper result = statement.executeQuery();
                if (result.next()) {
                    return result.getString(1);
                }
            }
            throw new SQLException("No value found with id " + id);
        }
    }
}