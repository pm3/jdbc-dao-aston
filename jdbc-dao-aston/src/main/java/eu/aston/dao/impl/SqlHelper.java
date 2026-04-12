package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.beanmeta.BeanMetaRegistry;
import eu.aston.dao.DaoException;
import eu.aston.dao.EntityConfig;
import eu.aston.dao.NoRowsException;
import eu.aston.dao.TooManyRowsException;

import javax.sql.DataSource;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Internal SQL execution helper — parsing, parameter binding, result mapping.
 */
public final class SqlHelper {

    private SqlHelper() {}

    // --- JDBC execution ---

    static <T> T queryOne(DataSource ds, ObjectMapper om, Class<T> type, Type genericType,
                          String sqlTemplate, Map<String, Object> params) {
        List<T> results = queryList(ds, om, type, genericType, sqlTemplate, params);
        if (results.isEmpty()) throw new NoRowsException("Expected 1 row, got 0");
        if (results.size() > 1) throw new TooManyRowsException("Expected 1 row, got " + results.size());
        return results.get(0);
    }

    static <T> Optional<T> queryOptional(DataSource ds, ObjectMapper om, Class<T> type, Type genericType,
                                          String sqlTemplate, Map<String, Object> params) {
        List<T> results = queryList(ds, om, type, genericType, sqlTemplate, params);
        if (results.isEmpty()) return Optional.empty();
        if (results.size() > 1) throw new TooManyRowsException("Expected 0-1 rows, got " + results.size());
        return Optional.of(results.get(0));
    }

    /** Pre-built column binding: colIndex + beanIndex + reader (scalar or JSON). */
    record BoundColumn(int colIndex, int beanIndex, JdbcBinder.ColumnReader reader, JavaType jsonType) {}

    @SuppressWarnings("unchecked")
    static <T> List<T> queryList(DataSource ds, ObjectMapper om, Class<T> type, Type genericType,
                                  String sqlTemplate, Map<String, Object> params) {
        var parsed = SqlTemplate.of(sqlTemplate).process(params);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed.params(), om);
            return readResults(ps, om, type);
        } catch (SQLException e) {
            throw new DaoException("Query failed: " + parsed.sql(), e);
        }
    }

    /** Fixed path: pre-built SQL + pre-resolved setters + values array. No template parsing. */
    static <T> List<T> queryListFixed(DataSource ds, ObjectMapper om, Class<T> type, Type genericType,
                                       String sql, JdbcBinder.ParamSetter[] setters, Object[] values) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) {
                JdbcBinder.setParam(ps, i + 1, values[i], setters[i], om);
            }
            return readResults(ps, om, type);
        } catch (SQLException e) {
            throw new DaoException("Query failed: " + sql, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> readResults(PreparedStatement ps, ObjectMapper om, Class<T> type) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            if (JdbcBinder.isScalar(type)) {
                JdbcBinder.ColumnReader reader = JdbcBinder.readerFor(type);
                var results = new ArrayList<T>();
                while (rs.next()) {
                    Object val = rs.getObject(1);
                    results.add(val == null ? null : (T) reader.read(rs, 1));
                }
                return results;
            }

            // Bean mapping — build column bindings once, iterate rows fast
            BeanMeta<T> beanMeta = BeanMetaRegistry.forClass(type);
            BoundColumn[] bindings = buildColumnBindings(beanMeta, rs.getMetaData(), om);
            int propCount = beanMeta.names().size();

            var results = new ArrayList<T>();
            while (rs.next()) {
                Object[] values = new Object[propCount];
                for (BoundColumn bc : bindings) {
                    if (bc.reader != null) {
                        Object val = rs.getObject(bc.colIndex);
                        values[bc.beanIndex] = val == null ? null : bc.reader.read(rs, bc.colIndex);
                    } else {
                        String json = rs.getString(bc.colIndex);
                        if (json != null) {
                            try {
                                values[bc.beanIndex] = om.readValue(json, bc.jsonType);
                            } catch (Exception e) {
                                throw new DaoException("JSON deserialization failed for column " + bc.colIndex, e);
                            }
                        }
                    }
                }
                results.add(beanMeta.create(values));
            }
            return results;
        }
    }

    /** Build column bindings: match RS columns to bean properties, pre-resolve readers. */
    private static <T> BoundColumn[] buildColumnBindings(BeanMeta<T> beanMeta, ResultSetMetaData meta,
                                                          ObjectMapper om) throws SQLException {
        List<String> names = beanMeta.names();
        List<Class<?>> types = beanMeta.types();
        List<Type> genericTypes = beanMeta.genericTypes();

        // Build RS column label → colIndex map
        int colCount = meta.getColumnCount();
        var colLabels = new HashMap<String, Integer>(colCount);
        for (int i = 1; i <= colCount; i++) {
            colLabels.put(meta.getColumnLabel(i).toLowerCase(), i);
        }

        // Match bean properties to RS columns
        var bindings = new ArrayList<BoundColumn>();
        for (int beanIdx = 0; beanIdx < names.size(); beanIdx++) {
            Integer colIndex = colLabels.get(names.get(beanIdx).toLowerCase());
            if (colIndex == null) continue;

            Class<?> propType = types.get(beanIdx);
            JdbcBinder.ColumnReader reader = JdbcBinder.readerFor(propType);
            JavaType jsonType = null;
            if (reader == null && om != null) {
                jsonType = om.getTypeFactory().constructType(genericTypes.get(beanIdx));
            }
            bindings.add(new BoundColumn(colIndex, beanIdx, reader, jsonType));
        }
        return bindings.toArray(new BoundColumn[0]);
    }

    static void execute(DataSource ds, ObjectMapper om, String sqlTemplate, Map<String, Object> params) {
        var parsed = SqlTemplate.of(sqlTemplate).process(params);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed.params(), om);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Execute failed: " + parsed.sql(), e);
        }
    }

    static int executeUpdate(DataSource ds, ObjectMapper om, String sqlTemplate, Map<String, Object> params) {
        var parsed = SqlTemplate.of(sqlTemplate).process(params);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed.params(), om);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("ExecuteUpdate failed: " + parsed.sql(), e);
        }
    }

    // --- Parameter binding ---

    private static void bindParams(PreparedStatement ps, List<Object> params, ObjectMapper om) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            JdbcBinder.setParam(ps, i + 1, params.get(i), om);
        }
    }

}
