package eu.aston.dao.impl;

import eu.aston.beanmeta.BeanMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Named parameter with known position in args array and lazily-resolved JDBC setter. Created once per method (static
 * field or proxy init), reused across all calls.
 */
public final class QueryParam {

    public final String name;
    public final int position;
    public final Class<?> type;
    private JdbcBinder.ParamSetter setter;

    public QueryParam(String name, int position, Class<?> type) {
        this.name = name;
        this.position = position;
        this.type = type;
    }

    /** Lazily resolve and cache the JDBC setter. Never null — returns JSON_SETTER for unknown types. */
    public JdbcBinder.ParamSetter setter() {
        if (setter == null) {
            setter = JdbcBinder.setterFor(type);
        }
        return setter;
    }

    /** Build QueryParam map from BeanMeta — each property gets a sequential position. */
    public static Map<String, QueryParam> fromBeanMeta(BeanMeta<?> meta) {
        List<String> names = meta.names();
        List<Class<?>> types = meta.types();
        var map = new HashMap<String, QueryParam>(names.size());
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i), new QueryParam(names.get(i), i, types.get(i)));
        }
        return Map.copyOf(map);
    }
}
