package nl.gogognome.dataaccess.dao;

import nl.gogognome.dataaccess.DataAccessException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

public class AbstractDomainClassDAOTest extends BaseInMemTransactionTest {

    private AuthorDAO authorDAO;
    private BookDAO bookDAO;

    @Before
    public void initDatabase() throws DataAccessException, SQLException, IOException {
        new TableDAO().createTablesAndSequences();
        authorDAO = new AuthorDAO();
        bookDAO = new BookDAO();
    }

    @Test
    public void whenNoAuthorsPresentThenFindAllReturnsEmptyList() throws SQLException {
        assertEquals(emptyList(), authorDAO.findAll());
    }

    @Test
    public void whenAuthorsPresentThenFindAllReturnsAllOfThem() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        List<Author> expectedList = asList(author1, author2, author3);
        List<Author> actualAuthors = authorDAO.findAll();

        assertAuthorsEqual(expectedList, actualAuthors);
    }

    @Test
    public void whenNoAuthorsPresentThenFindAllWithOrderReturnsEmptyList() throws SQLException {
        assertEquals(emptyList(), authorDAO.findAll("name"));
    }

    @Test
    public void whenAuthorsPresentThenFindAllWithOrderReturnsAllOfThemSorted() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        List<Author> expectedList = asList(author2, author3, author1);
        List<Author> actualAuthors = authorDAO.findAll("name");

        assertAuthorsEqual(expectedList, actualAuthors);
    }

    @Test
    public void whenAuthorDoesNotExistThenExistsReturnsFalse() throws SQLException {
        assertFalse(authorDAO.exists(1));
    }

    @Test
    public void whenAuthorExistsThenExistsReturnsTrue() throws SQLException {
        Author author = authorDAO.create(buildAuthor("Terry Pratchett"));

        assertTrue(authorDAO.exists(author.getId()));
    }

    @Test
    public void whenAuthorDoesNotExistThenExistsWithNameValuePairsReturnsFalse() throws SQLException {
        assertFalse(authorDAO.exists(new NameValuePairs().add("id", 1L)));
    }

    @Test
    public void whenAuthorExistsThenExistsWithNameValuePairsReturnsTrue() throws SQLException {
        Author author = authorDAO.create(buildAuthor("Terry Pratchett"));

        assertTrue(authorDAO.exists(new NameValuePairs().add("id", author.getId())));
    }

    @Test
    public void whenAuthorDoesNotExistThenExistsAtLeastOneReturnsFalse() throws SQLException {
        assertFalse(authorDAO.existsAtLeastOne(new NameValuePairs().add("name", "Terry Pratchett")));
    }

    @Test
    public void whenAuthorExistsThenExistsAtLeastOneReturnsTrue() throws SQLException {
        Author author = authorDAO.create(buildAuthor("Terry Pratchett"));

        assertTrue(authorDAO.existsAtLeastOne(new NameValuePairs().add("name", author.getName())));
    }

    @Test
    public void whenMultipleRecordExistsThenExistsAtLeastOneReturnsTrue() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor(author1.getName()));
        Author author3 = authorDAO.create(buildAuthor(author1.getName()));

        assertTrue(authorDAO.existsAtLeastOne(new NameValuePairs().add("name", author1.getName())));
    }

    @Test
    public void whenAuthorDoesNotExistThenFindReturnsNull() throws SQLException {
        assertNull(authorDAO.find(1));
    }

    @Test
    public void whenAuthorExistsThenFindReturnsIt() throws SQLException {
        Author author = authorDAO.create(buildAuthor("Terry Pratchett"));

        Author actualAuthor = authorDAO.find(author.getId());

        assertAuthorEqual(author, actualAuthor);
    }

    @Test(expected = NoRecordFoundException.class)
    public void whenAuthorDoesNotExistThenGetThrowsException() throws SQLException {
        assertNull(authorDAO.get(1));
    }

    @Test
    public void whenAuthorExistsThenGetReturnsIt() throws SQLException {
        Author author = authorDAO.create(buildAuthor("Terry Pratchett"));

        Author actualAuthor = authorDAO.get(author.getId());

        assertAuthorEqual(author, actualAuthor);
    }

    @Test
    public void whenAuthorDoesNotExistThenFindWithNameValuePairsReturnsNull() throws SQLException {
        assertNull(authorDAO.find(new NameValuePairs().add("id", 1L)));
    }

    @Test
    public void whenAuthorExistsThenFindWithNameValuePairsReturnsIt() throws SQLException {
        Author author = authorDAO.create(buildAuthor("Terry Pratchett"));

        Author actualAuthor = authorDAO.find(new NameValuePairs().add("id", author.getId()));

        assertAuthorEqual(author, actualAuthor);
    }

    @Test
    public void whenAuthorDoesNotExistFindAllWithWhereClauseReturnsEmptyList() throws SQLException {
        assertEquals(emptyList(), authorDAO.findAll(new NameValuePairs().add("name", "Terry Pratchett")));
    }

    @Test
    public void whenAuthorsExistFindAllWithWhereClauseReturnsAllMatchingRecords() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author3 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));

        List<Author> expectedList = asList(author1, author2);
        List<Author> actualAuthors = authorDAO.findAll(new NameValuePairs().add("name", "Terry Pratchett"));

        assertAuthorsEqual(expectedList, actualAuthors);
    }

    @Test
    public void whenAuthorsExistFindAllWithEmptyWhereClauseReturnsAllOfThem() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        List<Author> expectedList = asList(author1, author2, author3);
        List<Author> actualAuthors = authorDAO.findAll(new NameValuePairs());

        assertAuthorsEqual(expectedList, actualAuthors);
    }

    @Test
    public void whenAuthorsExistFindAllWithListOfStringsInWhereClauseReturnsAllMatchingAuthors() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        List<Author> expectedList = asList(author2, author3);
        List<Author> actualAuthors = authorDAO.findAll(new NameValuePairs().add("name", author2.getName(), author3.getName()));

        assertAuthorsEqual(expectedList, actualAuthors);
    }

    @Test
    public void whenAuthorDoesNotExistFindAllWithWhereClauseAndSortClauseReturnsEmptyList() throws SQLException {
        assertEquals(emptyList(), authorDAO.findAll(new NameValuePairs().add("name", "Terry Pratchett"), "id"));
    }

    @Test
    public void whenAuthorsExistFindAllWithWhereClauseAndSortClauseReturnsAllMatchingRecords() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author3 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));

        List<Author> expectedList = asList(author2, author1);
        List<Author> actualAuthors = authorDAO.findAll(new NameValuePairs().add("name", "Terry Pratchett"), "id desc");

        assertAuthorsEqual(expectedList, actualAuthors);
    }

    @Test
    public void whenAuthorsExistFindAllWithEmptyWhereClauseAndSortClauseReturnsAllOfThem() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        List<Author> expectedList = asList(author2, author3, author1);
        List<Author> actualAuthors = authorDAO.findAll(new NameValuePairs(), "name");

        assertAuthorsEqual(expectedList, actualAuthors);
    }

    @Test
    public void whenAuthorDoesNotExistFirstReturnsNull() throws SQLException {
        assertNull(authorDAO.first(new NameValuePairs().add("name", "Terry Pratchett")));
    }

    @Test
    public void whenAuthorsExistFirstClauseReturnsOneOfThem() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        Author actualAuthor = authorDAO.first(new NameValuePairs().add("name", author1.getName()));

        assertAuthorEqual(author1, actualAuthor);
    }

    @Test
    public void whenNoAuthersExistFindAllWhereReturnsEmptyList() throws SQLException {
        assertEquals(emptyList(), authorDAO.findAllWhere("name like 'J%'"));
    }

    @Test
    public void whenAuthersExistFindAllWhereReturnsMatchingAuthors() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        assertAuthorsEqual(asList(author2, author3), authorDAO.findAllWhere("name like 'J%'"));
    }

    @Test
    public void whenAuthorDoesNotExistHasAnyReturnsFalse() throws SQLException {
        assertFalse(authorDAO.hasAny());
    }

    @Test
    public void whenAuthorsExistHasAnyReturnsTrue() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        assertTrue(authorDAO.hasAny());
    }

    @Test
    public void whenNoAuthorsPresentDeleteWhereDeletesZeroAuthors() throws SQLException {
        assertEquals(0, authorDAO.deleteWhere(new NameValuePairs().add("name", "Terry Pratchett")));
    }

    @Test
    public void whenAuthorsPresentDeleteWhereDeletesMatchingAuthors() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        assertEquals(1, authorDAO.deleteWhere(new NameValuePairs().add("name", "Terry Pratchett")));

        assertAuthorsEqual(asList(author2, author3), authorDAO.findAll());
    }

    @Test(expected = NoRecordFoundException.class)
    public void whenNoAuthorsPresentDeleteThrowsException() throws SQLException {
        authorDAO.delete(1);
    }

    @Test
    public void whenAuthorsPresentDeleteDeletesMatchingAuthor() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        authorDAO.delete(author2.getId());

        assertAuthorsEqual(asList(author1, author3), authorDAO.findAll());
    }

    @Test
    public void whenObjectIsCreatedItsPrimaryKeyIsFilledIn() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));

        assertNotNull(author1.getId());
        assertEquals("Terry Pratchett", author1.getName());
    }

    @Test
    public void whenObjectIsCreatedItsPrimaryKeyIsUnique() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        Set<Long> ids = new HashSet<>();
        ids.add(author1.getId());
        ids.add(author2.getId());
        ids.add(author3.getId());
        assertEquals(3, ids.size());
    }

    @Test
    public void canCreateObjectWithNullValue() throws SQLException {
        Author author = authorDAO.create(buildAuthor(null));
        assertNull(author.getName());
    }

    @Test(expected = NoRecordFoundException.class)
    public void whenNonExistingAuthorIsUpdatedThenAnExceptionIsThrown() throws SQLException {
        Author author = buildAuthor("Terry Pratchett");
        author.setId(123);
        authorDAO.update(author);
    }

    @Test
    public void canUpdateValueWithNullValue() throws SQLException {
        Author author = authorDAO.create(buildAuthor("Terry Pratchett"));

        author.setName(null);
        authorDAO.update(author);

        assertNull(authorDAO.get(author.getId()).getName());
    }

    @Test
    public void whenExistingAuthorIsUpdatedAuthorIsUpdated() throws SQLException {
        Author author = buildAuthor("old name");
        author = authorDAO.create(author);

        author.setName("new name");
        authorDAO.update(author);

        assertEquals("new name", authorDAO.get(author.getId()).getName());
    }

    @Test
    public void whenTableIsEmptyCountReturnsZero() throws SQLException {
        assertEquals(0, authorDAO.count(null));
        assertEquals(0, authorDAO.count(new NameValuePairs().add("name", "Terry Pratchett")));
    }

    @Test
    public void whenTableIsNotEmptyCountReturnsNumberMatchingAuthors() throws SQLException {
        Author author1 = authorDAO.create(buildAuthor("Terry Pratchett"));
        Author author2 = authorDAO.create(buildAuthor("J.R.R. Tolkien"));
        Author author3 = authorDAO.create(buildAuthor("Joanne Rowling"));

        assertEquals(3, authorDAO.count(null));
        assertEquals(1, authorDAO.count(new NameValuePairs().add("name", "Terry Pratchett")));
    }

    @Test
    public void domainClassWithEnumAttributeCanBeStoredAndFetched() throws SQLException {
        Author tolkien = buildAuthor("J.R.R. Tolkien");
        tolkien = authorDAO.create(tolkien);

        Book lordOfTheRings = new Book();
        lordOfTheRings.setTitle("The Lord of the Rings");
        lordOfTheRings.setGenre(Book.Genre.FANTASY);
        lordOfTheRings.setAuthorId(tolkien.getId());
        lordOfTheRings = bookDAO.create(lordOfTheRings);

        Book bookFromDatabas = bookDAO.get(lordOfTheRings.getId());
        assertEquals(lordOfTheRings.getId(), bookFromDatabas.getId());
        assertEquals(lordOfTheRings.getTitle(), bookFromDatabas.getTitle());
        assertEquals(lordOfTheRings.getGenre(), bookFromDatabas.getGenre());
        assertEquals(lordOfTheRings.getAuthorId(), bookFromDatabas.getAuthorId());
    }

    private void assertAuthorEqual(Author author1, Author actualAuthor) {
        assertEquals(author1.getId(), actualAuthor.getId());
        assertEquals(author1.getName(), actualAuthor.getName());
    }

    private void assertAuthorsEqual(List<Author> expectedList, List<Author> actualAuthors) {
        assertListEquals(expectedList, actualAuthors, Author::getId);
        assertListEquals(expectedList, actualAuthors, Author::getName);
    }

    private static <T, U> void assertListEquals(List<T> expectedList, List<T> actualList, Function<T, U> function) {
        assertEquals(transform(expectedList, function), transform(actualList, function));
    }

    private static <FROM, TO> List<TO> transform(List<FROM> list, Function<FROM, TO> function) {
        return list.stream().map(function).collect(toList());
    }

    private Author buildAuthor(String name) {
        Author author = new Author();
        author.setName(name);
        return author;
    }

    private class TableDAO extends AbstractDAO {
        public TableDAO() throws DataAccessException {
            super("test");
        }

        public void createTablesAndSequences() throws SQLException, IOException {
            runScript(new InputStreamReader(getClass().getResourceAsStream("create_author_and_book.sql")), true);
        }
    }

    private class AuthorDAO extends AbstractDomainClassDAO<Author> {

        public AuthorDAO() throws DataAccessException {
            super("author", "author_sequence", "test");
        }

        @Override
        protected Author getObjectFromResultSet(ResultSetWrapper result) throws SQLException {
            Author author = new Author();
            author.setId(result.getLong("id"));
            author.setName(result.getString("name"));
            return author;
        }

        @Override
        protected NameValuePairs getNameValuePairs(Author domainObject) throws SQLException {
            return new NameValuePairs()
                    .add("id", domainObject.getId())
                    .add("name", domainObject.getName());
        }
    }

    private class BookDAO extends AbstractDomainClassDAO<Book> {

        public BookDAO() throws DataAccessException {
            super("book", "book_sequence", "test");
        }

        @Override
        protected Book getObjectFromResultSet(ResultSetWrapper result) throws SQLException {
            Book book = new Book();
            book.setId(result.getLong("id"));
            book.setTitle(result.getString("title"));
            book.setGenre(result.getEnum(Book.Genre.class, "genre"));
            book.setAuthorId(result.getLong("author_id"));
            return book;
        }

        @Override
        protected NameValuePairs getNameValuePairs(Book book) throws SQLException {
            return new NameValuePairs()
                    .add("id", book.getId())
                    .add("title", book.getTitle())
                    .add("genre", book.getGenre())
                    .add("author_id", book.getAuthorId());
        }
    }
}
