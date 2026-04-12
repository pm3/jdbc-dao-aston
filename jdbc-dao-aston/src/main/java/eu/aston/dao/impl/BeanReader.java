package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.beanmeta.BeanMetaRegistry;
import eu.aston.dao.DaoException;

import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cached bean reader — column templates and result mapping for any bean/record/DTO.
 * Cached per Class, reusable across queries.
 */
public final class BeanReader<T> {

    private static final ConcurrentHashMap<Class<?>, BeanReader<?>> cache = new ConcurrentHashMap<>();

    /** Column binding: colIndex + beanIndex + reader. genericType for JSON columns (reader==null). */
    record BoundColumn(int colIndex, int beanIndex, JdbcBinder.ColumnReader reader,
                       Type genericType, AtomicReference<JavaType> cachedJavaType) {}

    private final BeanMeta<T> meta;
    private final int propCount;
    /** Cached: lowercase(name) → BoundColumn with colIndex=-1 */
    private final Map<String, BoundColumn> columnTemplates;

    @SuppressWarnings("unchecked")
    public static <T> BeanReader<T> forClass(Class<T> type) {
        return (BeanReader<T>) cache.computeIfAbsent(type, BeanReader::new);
    }

    private BeanReader(Class<?> rawType) {
        @SuppressWarnings("unchecked")
        Class<T> type = (Class<T>) rawType;
        this.meta = BeanMetaRegistry.forClass(type);
        List<String> names = meta.names();
        List<Class<?>> types = meta.types();
        List<Type> genericTypes = meta.genericTypes();
        this.propCount = names.size();

        var map = new HashMap<String, BoundColumn>(propCount);
        for (int i = 0; i < propCount; i++) {
            JdbcBinder.ColumnReader reader = JdbcBinder.readerFor(types.get(i));
            Type genType = reader == null ? genericTypes.get(i) : null;
            var javaTypeRef = genType != null ? new AtomicReference<JavaType>() : null;
            map.put(names.get(i).toLowerCase(), new BoundColumn(-1, i, reader, genType, javaTypeRef));
        }
        this.columnTemplates = Map.copyOf(map);
    }

    public BeanMeta<T> meta() {
        return meta;
    }

    /** Match RS columns to cached templates, produce BoundColumn[] indexed by beanIndex. */
    BoundColumn[] resolveColumnBindings(ResultSetMetaData rsMeta) throws SQLException {
        BoundColumn[] bindings = new BoundColumn[propCount];
        int colCount = rsMeta.getColumnCount();
        for (int col = 1; col <= colCount; col++) {
            BoundColumn template = columnTemplates.get(rsMeta.getColumnLabel(col).toLowerCase());
            if (template != null) {
                bindings[template.beanIndex] = new BoundColumn(col, template.beanIndex, template.reader,
                        template.genericType, template.cachedJavaType);
            }
        }
        return bindings;
    }

    /** Read bean results from an already-executed PreparedStatement. */
    @SuppressWarnings("unchecked")
    <R> List<R> readBeanResults(PreparedStatement ps, ObjectMapper om) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            BoundColumn[] bindings = resolveColumnBindings(rs.getMetaData());

            var results = new ArrayList<R>();
            while (rs.next()) {
                Object[] values = new Object[propCount];
                for (int i = 0; i < propCount; i++) {
                    BoundColumn bc = bindings[i];
                    if (bc == null) continue;
                    if (bc.reader != null) {
                        values[i] = bc.reader.read(rs, bc.colIndex);
                    } else {
                        String json = rs.getString(bc.colIndex);
                        if (json != null) {
                            if (om == null) throw new DaoException("ObjectMapper required for JSON column at index " + bc.beanIndex);
                            try {
                                JavaType javaType = bc.cachedJavaType.get();
                                if (javaType == null) {
                                    javaType = om.getTypeFactory().constructType(bc.genericType);
                                    bc.cachedJavaType.set(javaType);
                                }
                                values[i] = om.readValue(json, javaType);
                            } catch (Exception e) {
                                throw new DaoException("JSON deserialization failed for column " + bc.colIndex, e);
                            }
                        }
                    }
                }
                results.add((R) meta.create(values));
            }
            return results;
        }
    }
}
