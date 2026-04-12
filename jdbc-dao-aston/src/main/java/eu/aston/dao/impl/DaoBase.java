package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.dao.EntityConfig;

import javax.sql.DataSource;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for compile-time generated DAO implementations.
 * Provides helper methods that generated code delegates to.
 */
public abstract class DaoBase {

    protected DataSource dataSource;
    protected ObjectMapper objectMapper;

    protected DaoBase() {}

    protected DaoBase(DataSource dataSource, ObjectMapper objectMapper) {
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
    }

    // --- Entity operations ---

    protected <T> T entityLoad(EntityConfig<T> config, Object pkValue) {
        return SqlHelper.entityLoad(dataSource, objectMapper, config, pkValue);
    }

    protected <T> void entityInsert(EntityConfig<T> config, T entity) {
        SqlHelper.entityInsert(dataSource, objectMapper, config, entity);
    }

    protected <T> void entityUpdate(EntityConfig<T> config, T entity) {
        SqlHelper.entityUpdate(dataSource, objectMapper, config, entity);
    }

    protected <T> void entitySave(EntityConfig<T> config, T entity) {
        SqlHelper.entitySave(dataSource, objectMapper, config, entity);
    }

    protected <T> void entityDelete(EntityConfig<T> config, Object entityOrPk) {
        SqlHelper.entityDelete(dataSource, objectMapper, config, entityOrPk);
    }

    // --- Query operations ---

    protected <T> T queryOne(Class<T> type, Type genericType, String sql, String[] names, Object[] values) {
        return SqlHelper.queryOne(dataSource, objectMapper, type, genericType, sql, buildParamMap(names, values));
    }

    protected <T> Optional<T> queryOptional(Class<T> type, Type genericType, String sql, String[] names, Object[] values) {
        return SqlHelper.queryOptional(dataSource, objectMapper, type, genericType, sql, buildParamMap(names, values));
    }

    protected <T> List<T> queryList(Class<T> type, Type genericType, String sql, String[] names, Object[] values) {
        return SqlHelper.queryList(dataSource, objectMapper, type, genericType, sql, buildParamMap(names, values));
    }

    protected void queryExecute(String sql, String[] names, Object[] values) {
        SqlHelper.execute(dataSource, objectMapper, sql, buildParamMap(names, values));
    }

    protected int queryUpdate(String sql, String[] names, Object[] values) {
        return SqlHelper.executeUpdate(dataSource, objectMapper, sql, buildParamMap(names, values));
    }

    private static Map<String, Object> buildParamMap(String[] names, Object[] values) {
        var map = new HashMap<String, Object>();
        if (names != null && values != null) {
            for (int i = 0; i < names.length; i++) {
                map.put(names[i], values[i]);
            }
        }
        return map;
    }
}
