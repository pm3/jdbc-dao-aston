package eu.aston.dao;

/**
 * Thrown when a query expects at most one row but multiple are returned.
 */
public class TooManyRowsException extends DaoException {

    public TooManyRowsException(String message) {
        super(message);
    }
}
