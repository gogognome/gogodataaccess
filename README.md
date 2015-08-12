# gogodataaccess
Gogo data access, a simple data access library for Java

## What does gogo data access offer?

* A simple transaction mechanism that can be used to manage JDBC connections.
* The transaction mechanism can be extened for non-JDBC transactions too.
* A simple DAO (Data Access Object) base class to write your own DAOs.
* A domain object DAO base class to quickly implement CRUD operations for your domain objects.

## What does gogo data access not offer?

* This library is not an ORM like Hibernate. The DAO base classes make it very easy to get ORM like features, but you still
have to do a little more work than Hibernate.
* Creating migration scripts. This library offers classes to run scripts, but you will have to manage migrations yourself.
Or use a library like Liquibase to manage your transactions if you don't mind learning another language to write migration scripts.

## Why writing another library if there is Hibernate or some other JPA implementation?

I don't like mixing domain classes with knowledge of the database. And I like explicit control over _what_ queries are executed 
and _when_ they are executed. There have been a few use cases where I actually had to work around Hibernate to get something
simple done. And I prefer writing SQL queries instead of HQL. I can test SQL queries with any SQL console; I can't test
HQL queries in a console.

## Code samples

### Register a DataSource

Before you can create transactions using the database a `DataSource` instance must be registered. You can register
one or even multiple `DataSource` instances. To distinguish between them, each `DataSource` must be registered
using a unique name.

    // Let's create a DataSource for an in-memory database using H2 (only included for the tests of this project)
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:bookstore_database");

    // Register the DataSource under the name "bookstore"
    CompositeDatasourceTransaction.registerDataSource("bookstore", dataSource);

### One way to start and end transactions

    // Start a transaction
    CurrentTransaction.create();

    // Do some thing interesting here
    
    // Commit and close the transaction
    CurrentTransaction.close(true);
    
To rollback and close the transaction use:

    CurrentTransaction.close(false);

### Another way to start and end transactions

The class `RunTransaction` contains static methods that start and close a connection. The transaction will be
committed when no exception is thrown or rolled back when an exception is thrown.

Here are some examples:

    // A transaction that does not return a value. Ideal for creating or updating records.
    RunTransaction.withoutResult(() -> /* do something interesting here */);
    
    // A transaction that returns a value.
    String result = RunTransaction.withResult(() -> /* get some string out of the database */);

### Implement Data Access Objects (DAOs) for domain classes

Imagine you build an application that does something with books and authors. Imagine the domain classes `Author` looks like this:

    public class Author {
    
        private long id;
        private String name;
    
        public long getId() {
            return id;
        }
    
        public void setId(long id) {
            this.id = id;
        }
    
        public String getName() {
            return name;
        }
    
        public void setName(String name) {
            this.name = name;
        }
    }
    
To build a DAO for `Author` you can extend `AbstractDomainClassDAO` and implement two methods.
`getObjectFromResultSet` builds an `Author` instance from a result set. It is called whenever one author
or a list of authors is retrieved from the database.
`getNameValuePairs` builds a map like structure that maps each attribute of an author to a column name and
column value. Name value pairs are used whenever a record must be created or updated.

The `AuthorDAO` could look like this:

    private class AuthorDAO extends AbstractDomainClassDAO<Author> {

        public AuthorDAO() throws DataAccessException {
            super("author", "author_sequence", "bookstore"); // name of table, name of sequence and name of DataSource
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

By just implementing these two methods your DAO is basically finished. It inherits a lot of methods from
`AbstractDomainClassDAO`, for example:

    exists(123L) // checks whether an author with id 123 exists
    create(author) // creates an author record. An instance of Author with the id generated by the sequence is returned
    update(author) // updates an existing author record
    find(123L) // get author with id 123. Returns null if it does not exist
    get(123L) // get author with id 123. Throws an exception if it does not exist
    delete(123L) // deletes author with id 123. Throws an exception if it does not exist
    findAll() // get a list with all authors
    findAllWhere("name like 'J%'") // get a list of all authors whose name start with a J
    find(nameValuePairs) // get a list of all authors with matching name value pairs

### Implement Data Access Objects (DAOs) for non-domain classes

If you want to get data from the database that does not correspond to domain classes, you can consider
building a subclass of `AbstractDAO`. Your class inherits many methods. Methods to insert or update records
in the database. And many methods to execute a query and transform the result set to a list, set or map.

Here is example code that shows the power of `AbstractDAO`:

    public class AuthorDAO {

        // Insert a record in the author table.
        public void createAuthor(long id, String name) throws SQLException {
            insert("author", new NameValuePairs().add("id", id).add("name", name));
        }
        
        // Updates a record while returning the number of modified rows
        public int updateAuthor(long id, String newName) throws SQLException {
            return execute("update author set name=? where id=?", name, id).getNumberModifiedRows();
        }
        
        // Find all author names using a pivot
        public List<String> findNames(List<Long> ids) throws SQLException {
            return execute("select name from author where id in (?)", ids).toList(r -> r.getString(1));
        }

        // Find a HashMap from id to name for all authors
        public HashMap<Long, String> getIdToName() throws SQLException {
            return execute("select id, name from author").toHashMap(r -> r.getLong(1), r -> r.getString(2));
        }
    }

Check out the `AbstractDAOTest` for more examples of this class.
