package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.beanmeta.BeanMetaRegistry;
import eu.aston.dao.DaoException;
import eu.aston.dao.EntityConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pre-computed entity CRUD operations. SQL, setters, and property mappings
 * are resolved once per EntityConfig; at runtime only values are extracted and bound.
 */
final class EntityBinder<T> {

    private static final ConcurrentHashMap<EntityConfig<?>, EntityBinder<?>> cache = new ConcurrentHashMap<>();

    /** Per-column metadata, resolved once at init. */
    record ColInfo(String name, JdbcBinder.ParamSetter setter, boolean isPk, boolean isTimestamp) {}

    private final BeanMeta<T> meta;
    private final EntityConfig<T> config;
    private final ColInfo[] columns;

    // update: pre-built (fixed shape — always same columns)
    private final String updateSql;
    private final int[] updateParamOrder; // column indices: non-PK first, PK last

    // delete / load
    private final String deleteSql;
    private final String loadSql;
    private final int pkColIdx;

    @SuppressWarnings("unchecked")
    static <T> EntityBinder<T> forConfig(EntityConfig<T> config) {
        return (EntityBinder<T>) cache.computeIfAbsent(config, EntityBinder::new);
    }

    private EntityBinder(EntityConfig<?> rawConfig) {
        @SuppressWarnings("unchecked")
        EntityConfig<T> config = (EntityConfig<T>) rawConfig;
        this.config = config;
        this.meta = BeanMetaRegistry.forClass(config.type());

        var names = meta.names();
        var types = meta.types();
        String table = config.table();
        String pk = config.pk();
        String createdAt = config.createdAt();
        String updatedAt = config.updatedAt();

        // Build column info array
        int pkIdx = -1;
        columns = new ColInfo[names.size()];
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            boolean isPk = name.equalsIgnoreCase(pk);
            boolean isTs = (createdAt != null && name.equalsIgnoreCase(createdAt))
                    || (updatedAt != null && name.equalsIgnoreCase(updatedAt));
            columns[i] = new ColInfo(name, JdbcBinder.setterFor(types.get(i)), isPk, isTs);
            if (isPk) pkIdx = i;
        }
        this.pkColIdx = pkIdx;

        // --- UPDATE (fixed shape: all non-PK columns + PK in WHERE) ---
        var setClauses = new ArrayList<String>();
        var upOrder = new ArrayList<Integer>();
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isPk) continue;
            setClauses.add(columns[i].name + "=?");
            upOrder.add(i);
        }
        upOrder.add(pkIdx);
        this.updateParamOrder = upOrder.stream().mapToInt(Integer::intValue).toArray();
        this.updateSql = "UPDATE " + table + " SET " + String.join(", ", setClauses)
                + " WHERE " + pk + "=?";

        // --- DELETE / LOAD ---
        this.deleteSql = "DELETE FROM " + table + " WHERE " + pk + "=?";
        this.loadSql = "SELECT * FROM " + table + " WHERE " + pk + "=?";
    }

    // --- Runtime operations ---

    void insert(DataSource ds, ObjectMapper om, T entity) {
        // Determine which columns to include (skip empty PK, skip null timestamps)
        var colNames = new ArrayList<String>();
        var values = new ArrayList<Object>();
        var setters = new ArrayList<JdbcBinder.ParamSetter>();

        for (ColInfo col : columns) {
            Object value = meta.get(entity, col.name);
            if (col.isPk) {
                boolean pkEmpty = value == null || (value instanceof Number n && n.longValue() == 0);
                if (pkEmpty) continue;
            }
            if (col.isTimestamp && value == null) continue;
            colNames.add(col.name);
            values.add(value);
            setters.add(col.setter);
        }

        String sql = "INSERT INTO " + config.table() + " (" + String.join(", ", colNames)
                + ") VALUES (" + String.join(", ", Collections.nCopies(colNames.size(), "?")) + ")";

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                JdbcBinder.setParam(ps, i + 1, values.get(i), setters.get(i), om);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Insert failed: " + sql, e);
        }
    }

    void update(DataSource ds, ObjectMapper om, T entity) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(updateSql)) {
            for (int paramPos = 0; paramPos < updateParamOrder.length; paramPos++) {
                int colIdx = updateParamOrder[paramPos];
                ColInfo col = columns[colIdx];
                Object value = meta.get(entity, col.name);
                JdbcBinder.setParam(ps, paramPos + 1, value, col.setter, om);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Update failed: " + updateSql, e);
        }
    }

    void save(DataSource ds, ObjectMapper om, T entity) {
        Object pkValue = pkColIdx >= 0 ? meta.get(entity, columns[pkColIdx].name) : null;
        boolean isEmpty = pkValue == null || (pkValue instanceof Number n && n.longValue() == 0);
        if (isEmpty) {
            insert(ds, om, entity);
        } else {
            update(ds, om, entity);
        }
    }

    void delete(DataSource ds, ObjectMapper om, Object entityOrPk) {
        Object pkValue;
        if (config.type().isInstance(entityOrPk)) {
            pkValue = meta.get(config.type().cast(entityOrPk), columns[pkColIdx].name);
        } else {
            pkValue = entityOrPk;
        }
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(deleteSql)) {
            JdbcBinder.setParam(ps, 1, pkValue, columns[pkColIdx].setter, om);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Delete failed: " + deleteSql, e);
        }
    }

    T load(DataSource ds, ObjectMapper om, Object pkValue) {
        var results = SqlHelper.queryListFixed(ds, om, config.type(), config.type(),
                loadSql, new JdbcBinder.ParamSetter[]{columns[pkColIdx].setter}, new Object[]{pkValue});
        if (results.isEmpty()) throw new eu.aston.dao.NoRowsException("Expected 1 row, got 0");
        if (results.size() > 1) throw new eu.aston.dao.TooManyRowsException("Expected 1 row, got " + results.size());
        return results.get(0);
    }
}
