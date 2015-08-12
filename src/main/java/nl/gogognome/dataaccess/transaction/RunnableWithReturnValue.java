package nl.gogognome.dataaccess.transaction;

public interface RunnableWithReturnValue<T> {

    T run() throws Exception;
}
