package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.dao.DaoException;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Typed JDBC parameter setters and column readers, resolved once by Class. Eliminates per-call instanceof chains in
 * setParam/readScalar.
 */
public final class JdbcBinder {

    @FunctionalInterface
    public interface ParamSetter {
        void set(PreparedStatement ps, int index, Object value) throws SQLException;
    }

    @FunctionalInterface
    public interface ColumnReader {
        Object read(ResultSet rs, int colIndex) throws SQLException;
    }

    private static final Map<Class<?>, ParamSetter> SETTERS;
    private static final Map<Class<?>, ColumnReader> READERS;

    static {
        var s = new HashMap<Class<?>, ParamSetter>();
        s.put(String.class, (ps, i, v) -> ps.setString(i, (String) v));
        s.put(Integer.class, (ps, i, v) -> ps.setInt(i, (Integer) v));
        s.put(int.class, (ps, i, v) -> ps.setInt(i, (Integer) v));
        s.put(Long.class, (ps, i, v) -> ps.setLong(i, (Long) v));
        s.put(long.class, (ps, i, v) -> ps.setLong(i, (Long) v));
        s.put(Boolean.class, (ps, i, v) -> ps.setBoolean(i, (Boolean) v));
        s.put(boolean.class, (ps, i, v) -> ps.setBoolean(i, (Boolean) v));
        s.put(Double.class, (ps, i, v) -> ps.setDouble(i, (Double) v));
        s.put(double.class, (ps, i, v) -> ps.setDouble(i, (Double) v));
        s.put(Float.class, (ps, i, v) -> ps.setFloat(i, (Float) v));
        s.put(float.class, (ps, i, v) -> ps.setFloat(i, (Float) v));
        s.put(Short.class, (ps, i, v) -> ps.setShort(i, (Short) v));
        s.put(short.class, (ps, i, v) -> ps.setShort(i, (Short) v));
        s.put(Byte.class, (ps, i, v) -> ps.setByte(i, (Byte) v));
        s.put(byte.class, (ps, i, v) -> ps.setByte(i, (Byte) v));
        s.put(BigDecimal.class, (ps, i, v) -> ps.setBigDecimal(i, (BigDecimal) v));
        s.put(Instant.class, (ps, i, v) -> ps.setTimestamp(i, Timestamp.from((Instant) v)));
        s.put(LocalDate.class, (ps, i, v) -> ps.setObject(i, v));
        s.put(LocalDateTime.class, (ps, i, v) -> ps.setObject(i, v));
        s.put(UUID.class, (ps, i, v) -> ps.setObject(i, v));
        s.put(byte[].class, (ps, i, v) -> ps.setBytes(i, (byte[]) v));
        SETTERS = Map.copyOf(s);

        var r = new HashMap<Class<?>, ColumnReader>();
        r.put(String.class, ResultSet::getString);
        r.put(Integer.class, ResultSet::getInt);
        r.put(int.class, ResultSet::getInt);
        r.put(Long.class, ResultSet::getLong);
        r.put(long.class, ResultSet::getLong);
        r.put(Boolean.class, ResultSet::getBoolean);
        r.put(boolean.class, ResultSet::getBoolean);
        r.put(Double.class, ResultSet::getDouble);
        r.put(double.class, ResultSet::getDouble);
        r.put(Float.class, ResultSet::getFloat);
        r.put(float.class, ResultSet::getFloat);
        r.put(Short.class, ResultSet::getShort);
        r.put(short.class, ResultSet::getShort);
        r.put(Byte.class, ResultSet::getByte);
        r.put(byte.class, ResultSet::getByte);
        r.put(BigDecimal.class, ResultSet::getBigDecimal);
        r.put(byte[].class, ResultSet::getBytes);
        r.put(UUID.class, (rs, i) -> rs.getObject(i, UUID.class));
        r.put(Instant.class, (rs, i) -> {
            Timestamp ts = rs.getTimestamp(i);
            return ts != null ? ts.toInstant() : null;
        });
        r.put(LocalDate.class, (rs, i) -> rs.getObject(i, LocalDate.class));
        r.put(LocalDateTime.class, (rs, i) -> rs.getObject(i, LocalDateTime.class));
        READERS = Map.copyOf(r);
    }

    private JdbcBinder() {
    }

    /**
     * JSON fallback setter — serializes value to JSON string via ObjectMapper stored in thread-local or passed
     * separately.
     */
    public static final ParamSetter JSON_SETTER = (ps, index, value) -> {
        // This setter requires ObjectMapper — use setParamWithOm for JSON columns
        throw new UnsupportedOperationException("JSON setter must be called via setParam with ObjectMapper");
    };

    /** Resolve setter by type. Returns JSON_SETTER for unknown types (never null). */
    public static ParamSetter setterFor(Class<?> type) {
        ParamSetter setter = SETTERS.get(type);
        return setter != null ? setter : JSON_SETTER;
    }

    /** Resolve setter by type, null for unknown. */
    public static ParamSetter scalarSetterFor(Class<?> type) {
        return SETTERS.get(type);
    }

    /** Resolve reader by type. Returns null for unknown types (JSON). */
    public static ColumnReader readerFor(Class<?> type) {
        return READERS.get(type);
    }

    /** Set param with null handling. Uses pre-resolved setter; JSON_SETTER triggers JSON serialization. */
    public static void setParam(PreparedStatement ps, int index, Object value, ParamSetter setter, ObjectMapper om)
            throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
            return;
        }
        if (setter != JSON_SETTER) {
            setter.set(ps, index, value);
            return;
        }
        // JSON fallback
        if (om == null)
            throw new DaoException("ObjectMapper required for JSON column (type: " + value.getClass().getName() + ")");
        try {
            String json = om.writeValueAsString(value);
            ps.setObject(index, json, Types.OTHER);
        } catch (Exception e) {
            throw new DaoException("JSON serialization failed", e);
        }
    }

    /** Set param by runtime type lookup — for dynamic template path. */
    public static void setParam(PreparedStatement ps, int index, Object value, ObjectMapper om) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
            return;
        }
        ParamSetter setter = SETTERS.get(value.getClass());
        if (setter != null) {
            setter.set(ps, index, value);
            return;
        }
        // JSON fallback
        if (om == null)
            throw new DaoException("ObjectMapper required for JSON column (type: " + value.getClass().getName() + ")");
        try {
            String json = om.writeValueAsString(value);
            ps.setObject(index, json, Types.OTHER);
        } catch (Exception e) {
            throw new DaoException("JSON serialization failed", e);
        }
    }

    /** Check if type is a known scalar (has a direct JDBC setter/reader). */
    public static boolean isScalar(Class<?> type) {
        return SETTERS.containsKey(type);
    }
}
