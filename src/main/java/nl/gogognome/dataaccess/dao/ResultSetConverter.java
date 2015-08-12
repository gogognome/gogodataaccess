package nl.gogognome.dataaccess.dao;

import java.sql.SQLException;

public interface ResultSetConverter<T>{
    T convert(ResultSetWrapper resultSet) throws SQLException;
}