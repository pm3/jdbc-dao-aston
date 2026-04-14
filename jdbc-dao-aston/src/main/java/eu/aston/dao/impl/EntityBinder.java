package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.dao.DaoException;
import eu.aston.dao.EntityConfig;
import eu.aston.dao.NoRowsException;
import eu.aston.dao.TooManyRowsException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pre-computed entity CRUD operations. SQL, setters, and property mappings are resolved once per EntityConfig; lives as
 * a lazy field on EntityConfig.
 */
public final class EntityBinder<T> {

    /** Per-column metadata, resolved once at init. */
    record ColInfo(String name, JdbcBinder.ParamSetter setter, boolean isPk, boolean isTimestamp) {
    }

    private final BeanMeta<T> meta;
    private final BeanReader<T> beanReader;
    private final EntityConfig<T> config;
    private final ColInfo[] columns;

    // insert: all columns including PK
    private final String insertSql;

    // insertWithoutPk: all columns except PK (for auto-increment via save)
    private final String insertWithoutPkSql;
    private final int[] insertWithoutPkParamOrder;

    // update: fixed shape — all non-PK columns + PK in WHERE
    private final String updateSql;
    private final int[] updateParamOrder; // column indices: non-PK first, PK last

    // delete / load
    private final String deleteSql;
    private final String loadSql;
    private final int pkColIdx;

    public EntityBinder(EntityConfig<T> config) {
        this.config = config;
        this.beanReader = BeanReader.forClass(config.type());
        this.meta = beanReader.meta();

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
            if (isPk)
                pkIdx = i;
        }
        this.pkColIdx = pkIdx;

        // --- INSERT (all columns including PK) ---
        {
            var colNames = new ArrayList<String>();
            for (ColInfo col : columns)
                colNames.add(col.name);
            this.insertSql = "INSERT INTO " + table + " (" + String.join(", ", colNames) + ") VALUES ("
                    + String.join(", ", Collections.nCopies(colNames.size(), "?")) + ")";
        }

        // --- INSERT without PK (for auto-increment via save) ---
        {
            var colNames = new ArrayList<String>();
            var order = new ArrayList<Integer>();
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].isPk)
                    continue;
                colNames.add(columns[i].name);
                order.add(i);
            }
            this.insertWithoutPkSql = "INSERT INTO " + table + " (" + String.join(", ", colNames) + ") VALUES ("
                    + String.join(", ", Collections.nCopies(colNames.size(), "?")) + ")";
            this.insertWithoutPkParamOrder = order.stream().mapToInt(Integer::intValue).toArray();
        }

        // --- UPDATE (fixed shape: all non-PK columns + PK in WHERE) ---
        var setClauses = new ArrayList<String>();
        var upOrder = new ArrayList<Integer>();
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isPk)
                continue;
            setClauses.add(columns[i].name + "=?");
            upOrder.add(i);
        }
        upOrder.add(pkIdx);
        this.updateParamOrder = upOrder.stream().mapToInt(Integer::intValue).toArray();
        this.updateSql = "UPDATE " + table + " SET " + String.join(", ", setClauses) + " WHERE " + pk + "=?";

        // --- DELETE / LOAD ---
        this.deleteSql = "DELETE FROM " + table + " WHERE " + pk + "=?";
        this.loadSql = "SELECT * FROM " + table + " WHERE " + pk + "=?";
    }

    // --- Runtime operations ---

    public void insertWithPk(DataSource ds, ObjectMapper om, T entity) {
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (int i = 0; i < columns.length; i++) {
                ColInfo col = columns[i];
                Object value = meta.get(entity, col.name);
                JdbcBinder.setParam(ps, i + 1, value, col.setter, om);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Insert failed: " + insertSql, e);
        }
    }

    public void update(DataSource ds, ObjectMapper om, T entity) {
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

    public void save(DataSource ds, ObjectMapper om, T entity) {
        Object pkValue = pkColIdx >= 0 ? meta.get(entity, columns[pkColIdx].name) : null;
        boolean isEmpty = pkValue == null || (pkValue instanceof Number n && n.longValue() == 0);
        if (isEmpty) {
            insertWithoutPk(ds, om, entity);
        } else {
            update(ds, om, entity);
        }
    }

    private void insertWithoutPk(DataSource ds, ObjectMapper om, T entity) {
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(insertWithoutPkSql)) {
            for (int paramPos = 0; paramPos < insertWithoutPkParamOrder.length; paramPos++) {
                int colIdx = insertWithoutPkParamOrder[paramPos];
                ColInfo col = columns[colIdx];
                Object value = meta.get(entity, col.name);
                JdbcBinder.setParam(ps, paramPos + 1, value, col.setter, om);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Insert failed: " + insertWithoutPkSql, e);
        }
    }

    public void delete(DataSource ds, ObjectMapper om, T entity) {
        Object pkValue = meta.get(entity, columns[pkColIdx].name);
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(deleteSql)) {
            JdbcBinder.setParam(ps, 1, pkValue, columns[pkColIdx].setter, om);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Delete failed: " + deleteSql, e);
        }
    }

    public T load(DataSource ds, ObjectMapper om, Object pkValue) {
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(loadSql)) {
            JdbcBinder.setParam(ps, 1, pkValue, columns[pkColIdx].setter, om);
            List<T> results = beanReader.readBeanResults(ps, om);
            if (results.isEmpty())
                throw new NoRowsException("Expected 1 row, got 0");
            if (results.size() > 1)
                throw new TooManyRowsException("Expected 1 row, got " + results.size());
            return results.get(0);
        } catch (NoRowsException | TooManyRowsException e) {
            throw e;
        } catch (Exception e) {
            throw new DaoException("Load failed: " + loadSql, e);
        }
    }
}
