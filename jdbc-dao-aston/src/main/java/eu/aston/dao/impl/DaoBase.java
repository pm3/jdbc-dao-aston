package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.beanmeta.BeanMetaRegistry;
import eu.aston.dao.EntityConfig;

import javax.sql.DataSource;
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
        return config.binder().load(dataSource, objectMapper, pkValue);
    }

    protected <T> void entityInsert(EntityConfig<T> config, T entity) {
        config.binder().insert(dataSource, objectMapper, entity);
    }

    protected <T> void entityUpdate(EntityConfig<T> config, T entity) {
        config.binder().update(dataSource, objectMapper, entity);
    }

    protected <T> void entitySave(EntityConfig<T> config, T entity) {
        config.binder().save(dataSource, objectMapper, entity);
    }

    protected <T> void entityDelete(EntityConfig<T> config, Object entityOrPk) {
        config.binder().delete(dataSource, objectMapper, entityOrPk);
    }

    // --- Query operations ---

    protected <T> T queryOne(Class<T> type, String sql, Map<String, QueryParam> paramDefs, Object[] args) {
        return SqlHelper.queryOne(dataSource, objectMapper, type, sql, paramDefs, args);
    }

    protected <T> Optional<T> queryOptional(Class<T> type, String sql, Map<String, QueryParam> paramDefs, Object[] args) {
        return SqlHelper.queryOptional(dataSource, objectMapper, type, sql, paramDefs, args);
    }

    protected <T> List<T> queryList(Class<T> type, String sql, Map<String, QueryParam> paramDefs, Object[] args) {
        return SqlHelper.queryList(dataSource, objectMapper, type, sql, paramDefs, args);
    }

    protected void queryExecute(String sql, Map<String, QueryParam> paramDefs, Object[] args) {
        SqlHelper.execute(dataSource, objectMapper, sql, paramDefs, args);
    }

    protected int queryUpdate(String sql, Map<String, QueryParam> paramDefs, Object[] args) {
        return SqlHelper.executeUpdate(dataSource, objectMapper, sql, paramDefs, args);
    }

    /** Expand a single bean/record into an Object[] matching QueryParam positions. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected static Object[] expandBeanArgs(Map<String, QueryParam> paramDefs, Object bean) {
        if (bean == null) return new Object[paramDefs.size()];
        BeanMeta meta = BeanMetaRegistry.forClass(bean.getClass());
        Object[] args = new Object[paramDefs.size()];
        for (var entry : paramDefs.entrySet()) {
            args[entry.getValue().position] = meta.get(bean, entry.getKey());
        }
        return args;
    }
}
