package eu.aston.dao;

/**
 * Thrown when a query expects exactly one row but none are returned.
 */
public class NoRowsException extends DaoException {

    public NoRowsException(String message) {
        super(message);
    }
}
