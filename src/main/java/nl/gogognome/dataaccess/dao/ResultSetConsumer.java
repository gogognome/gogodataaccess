package nl.gogognome.dataaccess.dao;

import java.sql.SQLException;

public interface ResultSetConsumer {
	void consume(ResultSetWrapper resultSet) throws SQLException;
}