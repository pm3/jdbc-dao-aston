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
 * Pre-computed entity CRUD operations. SQL, setters, and property mappings
 * are resolved once per EntityConfig; lives as a lazy field on EntityConfig.
 */
public final class EntityBinder<T> {

    /** Per-column metadata, resolved once at init. */
    record ColInfo(String name, JdbcBinder.ParamSetter setter, boolean isPk, boolean isTimestamp) {}

    /** Pre-computed insert variant (SQL + column order) keyed by skip-bitmask. */
    record InsertVariant(String sql, int[] paramOrder) {}

    private final BeanMeta<T> meta;
    private final BeanReader<T> beanReader;
    private final EntityConfig<T> config;
    private final ColInfo[] columns;

    // insert: pre-built variants for each combination of skippable columns
    private final int[] skippableColIndices; // indices of PK and timestamp columns
    private final InsertVariant[] insertVariants; // 2^skippable entries, indexed by include-bitmask

    // update: pre-built (fixed shape — always same columns)
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
            if (isPk) pkIdx = i;
        }
        this.pkColIdx = pkIdx;

        // --- INSERT (pre-compute all variants of skippable columns) ---
        var skipList = new ArrayList<Integer>();
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isPk || columns[i].isTimestamp) skipList.add(i);
        }
        this.skippableColIndices = skipList.stream().mapToInt(Integer::intValue).toArray();
        int variantCount = 1 << skippableColIndices.length;
        this.insertVariants = new InsertVariant[variantCount];
        for (int mask = 0; mask < variantCount; mask++) {
            var colNames = new ArrayList<String>();
            var order = new ArrayList<Integer>();
            for (int i = 0; i < columns.length; i++) {
                int skipBit = skipBitFor(i);
                if (skipBit >= 0 && (mask & (1 << skipBit)) == 0) continue; // skipped
                colNames.add(columns[i].name);
                order.add(i);
            }
            String sql = "INSERT INTO " + table + " (" + String.join(", ", colNames)
                    + ") VALUES (" + String.join(", ", Collections.nCopies(colNames.size(), "?")) + ")";
            insertVariants[mask] = new InsertVariant(sql, order.stream().mapToInt(Integer::intValue).toArray());
        }

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

    public void insert(DataSource ds, ObjectMapper om, T entity) {
        // Compute include-bitmask for skippable columns
        int mask = 0;
        for (int bit = 0; bit < skippableColIndices.length; bit++) {
            int colIdx = skippableColIndices[bit];
            ColInfo col = columns[colIdx];
            Object value = meta.get(entity, col.name);
            boolean include;
            if (col.isPk) {
                include = value != null && !(value instanceof Number n && n.longValue() == 0);
            } else {
                include = value != null; // timestamp
            }
            if (include) mask |= (1 << bit);
        }

        InsertVariant variant = insertVariants[mask];
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(variant.sql)) {
            for (int paramPos = 0; paramPos < variant.paramOrder.length; paramPos++) {
                int colIdx = variant.paramOrder[paramPos];
                ColInfo col = columns[colIdx];
                Object value = meta.get(entity, col.name);
                JdbcBinder.setParam(ps, paramPos + 1, value, col.setter, om);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Insert failed: " + variant.sql, e);
        }
    }

    /** Returns the bit position of colIdx in skippableColIndices, or -1 if not skippable. */
    private int skipBitFor(int colIdx) {
        for (int bit = 0; bit < skippableColIndices.length; bit++) {
            if (skippableColIndices[bit] == colIdx) return bit;
        }
        return -1;
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
            insert(ds, om, entity);
        } else {
            update(ds, om, entity);
        }
    }

    public void delete(DataSource ds, ObjectMapper om, Object entityOrPk) {
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

    public T load(DataSource ds, ObjectMapper om, Object pkValue) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(loadSql)) {
            JdbcBinder.setParam(ps, 1, pkValue, columns[pkColIdx].setter, om);
            List<T> results = beanReader.readBeanResults(ps, om);
            if (results.isEmpty()) throw new NoRowsException("Expected 1 row, got 0");
            if (results.size() > 1) throw new TooManyRowsException("Expected 1 row, got " + results.size());
            return results.get(0);
        } catch (NoRowsException | TooManyRowsException e) {
            throw e;
        } catch (Exception e) {
            throw new DaoException("Load failed: " + loadSql, e);
        }
    }
}
