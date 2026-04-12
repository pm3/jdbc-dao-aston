package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.beanmeta.BeanMetaRegistry;
import eu.aston.dao.DaoException;
import eu.aston.dao.EntityConfig;
import eu.aston.dao.ICondition;
import eu.aston.dao.NoRowsException;
import eu.aston.dao.Spread;
import eu.aston.dao.TooManyRowsException;

import javax.sql.DataSource;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Internal SQL execution helper — parsing, parameter binding, result mapping.
 */
public final class SqlHelper {

    private static final Pattern NAMED_PARAM = Pattern.compile(":(\\w+)");
    private static final Pattern OPTIONAL_BLOCK = Pattern.compile("/\\*\\*(.*?)\\*\\*/", Pattern.DOTALL);
    private static final Set<Class<?>> SCALAR_TYPES = Set.of(
            String.class, Boolean.class, boolean.class,
            Integer.class, int.class, Long.class, long.class,
            Short.class, short.class, Byte.class, byte.class,
            Float.class, float.class, Double.class, double.class,
            BigDecimal.class, Instant.class, LocalDate.class, LocalDateTime.class,
            UUID.class, byte[].class
    );

    private SqlHelper() {}

    static boolean isScalar(Class<?> type) {
        return SCALAR_TYPES.contains(type);
    }

    static boolean needsJson(Class<?> type) {
        return !isScalar(type) && type != Object.class;
    }

    // --- SQL template processing ---

    record ParsedSql(String sql, List<Object> params) {}

    static ParsedSql processSql(String template, Map<String, Object> namedParams) {
        // Step 1: process optional blocks /** ... **/
        String sql = processOptionalBlocks(template, namedParams);

        // Step 2: replace named params with ? and collect positional params
        var positionalParams = new ArrayList<Object>();
        var sb = new StringBuilder();
        var matcher = NAMED_PARAM.matcher(sql);
        while (matcher.find()) {
            String paramName = matcher.group(1);
            Object value = namedParams.get(paramName);
            if (value instanceof Spread<?> spread) {
                var placeholders = String.join(",", java.util.Collections.nCopies(spread.values().size(), "?"));
                matcher.appendReplacement(sb, placeholders);
                positionalParams.addAll(spread.values());
            } else if (value instanceof ICondition cond) {
                String condSql = cond.sql().isEmpty() ? "1=1" : cond.sql();
                matcher.appendReplacement(sb, Matcher.quoteReplacement(condSql));
                positionalParams.addAll(cond.params());
            } else {
                matcher.appendReplacement(sb, "?");
                positionalParams.add(value);
            }
        }
        matcher.appendTail(sb);

        return new ParsedSql(sb.toString(), positionalParams);
    }

    private static String processOptionalBlocks(String sql, Map<String, Object> namedParams) {
        var matcher = OPTIONAL_BLOCK.matcher(sql);
        var sb = new StringBuilder();
        while (matcher.find()) {
            String block = matcher.group(1);
            // check if any named param in this block is null
            var paramMatcher = NAMED_PARAM.matcher(block);
            boolean hasNull = false;
            while (paramMatcher.find()) {
                String paramName = paramMatcher.group(1);
                if (namedParams.get(paramName) == null) {
                    hasNull = true;
                    break;
                }
            }
            matcher.appendReplacement(sb, hasNull ? "" : Matcher.quoteReplacement(block));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

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

    @SuppressWarnings("unchecked")
    static <T> List<T> queryList(DataSource ds, ObjectMapper om, Class<T> type, Type genericType,
                                  String sqlTemplate, Map<String, Object> params) {
        var parsed = processSql(sqlTemplate, params);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed.params(), om);
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                if (isScalar(type)) {
                    var results = new ArrayList<T>();
                    while (rs.next()) {
                        results.add((T) readScalar(rs, 1, type));
                    }
                    return results;
                }

                // Bean mapping
                BeanMeta<T> beanMeta = BeanMetaRegistry.forClass(type);
                var nameIndexMap = buildNameIndexMap(beanMeta.names(), meta);

                var results = new ArrayList<T>();
                while (rs.next()) {
                    results.add(readBean(rs, beanMeta, nameIndexMap, om));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new DaoException("Query failed: " + parsed.sql(), e);
        }
    }

    static void execute(DataSource ds, ObjectMapper om, String sqlTemplate, Map<String, Object> params) {
        var parsed = processSql(sqlTemplate, params);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed.params(), om);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Execute failed: " + parsed.sql(), e);
        }
    }

    static int executeUpdate(DataSource ds, ObjectMapper om, String sqlTemplate, Map<String, Object> params) {
        var parsed = processSql(sqlTemplate, params);
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
            Object value = params.get(i);
            setParam(ps, i + 1, value, om);
        }
    }

    static void setParam(PreparedStatement ps, int index, Object value, ObjectMapper om) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
        } else if (value instanceof String s) {
            ps.setString(index, s);
        } else if (value instanceof Integer n) {
            ps.setInt(index, n);
        } else if (value instanceof Long n) {
            ps.setLong(index, n);
        } else if (value instanceof Boolean b) {
            ps.setBoolean(index, b);
        } else if (value instanceof Double d) {
            ps.setDouble(index, d);
        } else if (value instanceof Float f) {
            ps.setFloat(index, f);
        } else if (value instanceof Short s) {
            ps.setShort(index, s);
        } else if (value instanceof Byte b) {
            ps.setByte(index, b);
        } else if (value instanceof BigDecimal bd) {
            ps.setBigDecimal(index, bd);
        } else if (value instanceof Instant inst) {
            ps.setTimestamp(index, Timestamp.from(inst));
        } else if (value instanceof LocalDate ld) {
            ps.setObject(index, ld);
        } else if (value instanceof LocalDateTime ldt) {
            ps.setObject(index, ldt);
        } else if (value instanceof UUID uuid) {
            ps.setObject(index, uuid);
        } else if (value instanceof byte[] bytes) {
            ps.setBytes(index, bytes);
        } else {
            // JSON serialization
            if (om == null) throw new DaoException("ObjectMapper required for JSON column (type: " + value.getClass().getName() + ")");
            try {
                String json = om.writeValueAsString(value);
                // Use Types.OTHER for PostgreSQL JSONB compatibility
                ps.setObject(index, json, Types.OTHER);
            } catch (Exception e) {
                throw new DaoException("JSON serialization failed", e);
            }
        }
    }

    // --- Result mapping ---

    private static Map<String, Integer> buildNameIndexMap(List<String> beanNames, ResultSetMetaData meta) throws SQLException {
        int colCount = meta.getColumnCount();
        var colNames = new HashMap<String, Integer>(colCount);
        for (int i = 1; i <= colCount; i++) {
            colNames.put(meta.getColumnLabel(i).toLowerCase(), i);
        }
        var map = new HashMap<String, Integer>();
        for (String name : beanNames) {
            Integer colIndex = colNames.get(name.toLowerCase());
            if (colIndex != null) {
                map.put(name, colIndex);
            }
        }
        return map;
    }

    private static <T> T readBean(ResultSet rs, BeanMeta<T> beanMeta, Map<String, Integer> nameIndexMap,
                                   ObjectMapper om) throws SQLException {
        List<String> names = beanMeta.names();
        List<Class<?>> types = beanMeta.types();
        List<java.lang.reflect.Type> genericTypes = beanMeta.genericTypes();
        Object[] values = new Object[names.size()];

        for (int i = 0; i < names.size(); i++) {
            Integer colIndex = nameIndexMap.get(names.get(i));
            if (colIndex == null) {
                values[i] = null;
                continue;
            }
            Class<?> propType = types.get(i);
            if (isScalar(propType)) {
                values[i] = readScalar(rs, colIndex, propType);
            } else {
                // JSON deserialization
                String json = rs.getString(colIndex);
                if (json == null) {
                    values[i] = null;
                } else {
                    if (om == null) throw new DaoException("ObjectMapper required for JSON column: " + names.get(i));
                    try {
                        JavaType javaType = om.getTypeFactory().constructType(genericTypes.get(i));
                        values[i] = om.readValue(json, javaType);
                    } catch (Exception e) {
                        throw new DaoException("JSON deserialization failed for: " + names.get(i), e);
                    }
                }
            }
        }
        return beanMeta.create(values);
    }

    private static Object readScalar(ResultSet rs, int colIndex, Class<?> type) throws SQLException {
        Object val = rs.getObject(colIndex);
        if (val == null) return null;

        if (type == String.class) return rs.getString(colIndex);
        if (type == int.class || type == Integer.class) return rs.getInt(colIndex);
        if (type == long.class || type == Long.class) return rs.getLong(colIndex);
        if (type == boolean.class || type == Boolean.class) return rs.getBoolean(colIndex);
        if (type == double.class || type == Double.class) return rs.getDouble(colIndex);
        if (type == float.class || type == Float.class) return rs.getFloat(colIndex);
        if (type == short.class || type == Short.class) return rs.getShort(colIndex);
        if (type == byte.class || type == Byte.class) return rs.getByte(colIndex);
        if (type == BigDecimal.class) return rs.getBigDecimal(colIndex);
        if (type == byte[].class) return rs.getBytes(colIndex);
        if (type == UUID.class) return rs.getObject(colIndex, UUID.class);
        if (type == Instant.class) {
            Timestamp ts = rs.getTimestamp(colIndex);
            return ts != null ? ts.toInstant() : null;
        }
        if (type == LocalDate.class) return rs.getObject(colIndex, LocalDate.class);
        if (type == LocalDateTime.class) return rs.getObject(colIndex, LocalDateTime.class);
        return val;
    }

    // --- Entity CRUD helpers ---

    static <T> T entityLoad(DataSource ds, ObjectMapper om, EntityConfig<T> config, Object pkValue) {
        BeanMeta<T> meta = BeanMetaRegistry.forClass(config.type());
        String sql = "SELECT * FROM " + config.table() + " WHERE " + config.pk() + "=:pk";
        return queryOne(ds, om, config.type(), config.type(), sql, Map.of("pk", pkValue));
    }

    static <T> void entityInsert(DataSource ds, ObjectMapper om, EntityConfig<T> config, T entity) {
        BeanMeta<T> meta = BeanMetaRegistry.forClass(config.type());
        List<String> names = meta.names();
        List<String> insertNames = new ArrayList<>();
        List<Object> insertValues = new ArrayList<>();

        for (String name : names) {
            Object value = meta.get(entity, name);
            // skip PK if empty (null or 0) — let DB default/sequence handle it
            if (name.equalsIgnoreCase(config.pk())) {
                boolean pkEmpty = value == null || (value instanceof Number n && n.longValue() == 0);
                if (pkEmpty) continue;
            }
            // skip createdAt/updatedAt if null — let DB default handle it
            if (value == null && (name.equalsIgnoreCase(config.createdAt()) || name.equalsIgnoreCase(config.updatedAt()))) {
                continue;
            }
            insertNames.add(name);
            insertValues.add(value);
        }

        var colList = String.join(", ", insertNames);
        var placeholders = String.join(", ", java.util.Collections.nCopies(insertNames.size(), "?"));
        String sql = "INSERT INTO " + config.table() + " (" + colList + ") VALUES (" + placeholders + ")";

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < insertValues.size(); i++) {
                setParam(ps, i + 1, insertValues.get(i), om);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Insert failed: " + sql, e);
        }
    }

    static <T> void entityUpdate(DataSource ds, ObjectMapper om, EntityConfig<T> config, T entity) {
        BeanMeta<T> meta = BeanMetaRegistry.forClass(config.type());
        List<String> names = meta.names();
        var setClauses = new ArrayList<String>();
        var values = new ArrayList<Object>();
        Object pkValue = null;

        for (String name : names) {
            Object value = meta.get(entity, name);
            if (name.equalsIgnoreCase(config.pk())) {
                pkValue = value;
                continue;
            }
            setClauses.add(name + "=?");
            values.add(value);
        }

        if (pkValue == null) throw new DaoException("Primary key is null for update");
        values.add(pkValue);

        String sql = "UPDATE " + config.table() + " SET " + String.join(", ", setClauses)
                + " WHERE " + config.pk() + "=?";

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                setParam(ps, i + 1, values.get(i), om);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Update failed: " + sql, e);
        }
    }

    static <T> void entitySave(DataSource ds, ObjectMapper om, EntityConfig<T> config, T entity) {
        BeanMeta<T> meta = BeanMetaRegistry.forClass(config.type());
        Object pkValue = meta.get(entity, findProperty(meta, config.pk()));
        boolean isEmpty = pkValue == null
                || (pkValue instanceof Number n && n.longValue() == 0);
        if (isEmpty) {
            entityInsert(ds, om, config, entity);
        } else {
            entityUpdate(ds, om, config, entity);
        }
    }

    static <T> void entityDelete(DataSource ds, ObjectMapper om, EntityConfig<T> config, Object entityOrPk) {
        Object pkValue;
        if (config.type().isInstance(entityOrPk)) {
            BeanMeta<T> meta = BeanMetaRegistry.forClass(config.type());
            pkValue = meta.get(config.type().cast(entityOrPk), findProperty(meta, config.pk()));
        } else {
            pkValue = entityOrPk;
        }
        String sql = "DELETE FROM " + config.table() + " WHERE " + config.pk() + "=:pk";
        execute(ds, om, sql, Map.of("pk", pkValue));
    }

    /** Find the bean property name that matches a column name (case-insensitive). */
    static String findProperty(BeanMeta<?> meta, String columnName) {
        for (String name : meta.names()) {
            if (name.equalsIgnoreCase(columnName)) return name;
        }
        return columnName;
    }
}
