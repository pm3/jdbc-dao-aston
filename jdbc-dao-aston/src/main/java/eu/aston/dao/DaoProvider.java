package eu.aston.dao;

import javax.sql.DataSource;

/**
 * ServiceLoader interface for compile-time generated DAO implementations.
 */
public interface DaoProvider {

    /** The DAO interface this provider implements. */
    Class<?> daoInterface();

    /** Create a new configured instance of the DAO. */
    Object newInstance(DataSource dataSource, Object objectMapper);
}
