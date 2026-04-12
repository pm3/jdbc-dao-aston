package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.beanmeta.BeanMetaRegistry;
import eu.aston.dao.DaoException;
import eu.aston.dao.EntityConfig;
import eu.aston.dao.Query;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reflection-based proxy InvocationHandler for DAO interfaces.
 * All resolution happens eagerly in the constructor — invoke() is a straight lookup + call.
 */
public final class DaoProxy implements InvocationHandler {

    private final Map<Method, MethodExecutor> executors;

    public DaoProxy(Class<?> daoInterface, DataSource ds, ObjectMapper om) {
        var entityConfigs = scanEntityConfigs(daoInterface);
        var map = new HashMap<Method, MethodExecutor>();
        for (Method method : daoInterface.getMethods()) {
            if (method.getDeclaringClass() == Object.class) continue;
            if (method.isDefault()) continue;
            map.put(method, buildExecutor(method, ds, om, entityConfigs));
        }
        this.executors = Map.copyOf(map);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        MethodExecutor executor = executors.get(method);
        return executor != null ? executor.execute(args) : method.invoke(this, args);
    }

    // --- Executor building (runs once per method in constructor) ---

    private static MethodExecutor buildExecutor(Method method, DataSource ds, ObjectMapper om,
                                                 Map<Class<?>, EntityConfig<?>> entityConfigs) {
        Query queryAnn = method.getAnnotation(Query.class);
        if (queryAnn != null) {
            return buildQueryExecutor(method, queryAnn.value(), ds, om);
        }
        return buildEntityExecutor(method, ds, om, entityConfigs);
    }

    // --- @Query methods ---

    private static MethodExecutor buildQueryExecutor(Method method, String sql, DataSource ds, ObjectMapper om) {
        String[] paramNames = getParamNames(method);
        Class<?> type = resolveElementType(method);

        boolean expandBean = paramNames.length == 1
                && !JdbcBinder.isScalar(method.getParameterTypes()[0])
                && !eu.aston.dao.Spread.class.isAssignableFrom(method.getParameterTypes()[0])
                && !eu.aston.dao.ICondition.class.isAssignableFrom(method.getParameterTypes()[0]);

        Map<String, QueryParam> paramDefs;
        ArgsMapper argsMapper;
        if (expandBean) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            BeanMeta beanMeta = BeanMetaRegistry.forClass(method.getParameterTypes()[0]);
            paramDefs = QueryParam.fromBeanMeta(beanMeta);
            Map<String, QueryParam> pd = paramDefs; // effectively final
            argsMapper = args -> extractBeanArgs(beanMeta, pd, args[0]);
        } else {
            paramDefs = buildQueryParams(paramNames, method.getParameterTypes());
            argsMapper = args -> args;
        }

        // Pre-select the exact SqlHelper method — no switch at runtime
        Map<String, QueryParam> pd = paramDefs;
        return switch (resolveReturnKind(method)) {
            case VOID -> args -> { SqlHelper.execute(ds, om, sql, pd, argsMapper.map(args)); return null; };
            case INT -> args -> SqlHelper.executeUpdate(ds, om, sql, pd, argsMapper.map(args));
            case ONE -> args -> SqlHelper.queryOne(ds, om, type, sql, pd, argsMapper.map(args));
            case OPTIONAL -> args -> SqlHelper.queryOptional(ds, om, type, sql, pd, argsMapper.map(args));
            case LIST -> args -> SqlHelper.queryList(ds, om, type, sql, pd, argsMapper.map(args));
        };
    }

    // --- Entity convention methods ---

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static MethodExecutor buildEntityExecutor(Method method, DataSource ds, ObjectMapper om,
                                                       Map<Class<?>, EntityConfig<?>> entityConfigs) {
        String name = method.getName();
        if (name.startsWith("load")) {
            EntityConfig<?> config = findConfig(entityConfigs, method.getReturnType());
            return args -> config.binder().load(ds, om, args[0]);
        }
        if (name.startsWith("insert")) {
            EntityConfig config = findConfig(entityConfigs, method.getParameterTypes()[0]);
            return args -> { config.binder().insert(ds, om, args[0]); return null; };
        }
        if (name.startsWith("update")) {
            EntityConfig config = findConfig(entityConfigs, method.getParameterTypes()[0]);
            return args -> { config.binder().update(ds, om, args[0]); return null; };
        }
        if (name.startsWith("save")) {
            EntityConfig config = findConfig(entityConfigs, method.getParameterTypes()[0]);
            return args -> { config.binder().save(ds, om, args[0]); return null; };
        }
        if (name.startsWith("delete")) {
            EntityConfig config = resolveDeleteConfig(entityConfigs, method);
            return args -> { config.binder().delete(ds, om, args[0]); return null; };
        }
        throw new DaoException("Cannot resolve method: " + name
                + " — must have @Query or start with load/insert/update/save/delete");
    }

    // --- Helpers ---

    @SuppressWarnings("rawtypes")
    private static EntityConfig resolveDeleteConfig(Map<Class<?>, EntityConfig<?>> entityConfigs, Method method) {
        if (method.getParameterTypes().length == 1) {
            EntityConfig<?> config = entityConfigs.get(method.getParameterTypes()[0]);
            if (config != null) return config;
            if (entityConfigs.size() == 1) return entityConfigs.values().iterator().next();
        }
        throw new DaoException("Cannot resolve delete entity for method: " + method.getName());
    }

    private static EntityConfig<?> findConfig(Map<Class<?>, EntityConfig<?>> entityConfigs, Class<?> beanType) {
        EntityConfig<?> config = entityConfigs.get(beanType);
        if (config == null) throw new DaoException("No EntityConfig found for type: " + beanType.getName());
        return config;
    }

    private static Map<Class<?>, EntityConfig<?>> scanEntityConfigs(Class<?> daoInterface) {
        var configs = new LinkedHashMap<Class<?>, EntityConfig<?>>();
        for (var field : daoInterface.getDeclaredFields()) {
            if (EntityConfig.class.isAssignableFrom(field.getType())) {
                try {
                    field.setAccessible(true);
                    EntityConfig<?> config = (EntityConfig<?>) field.get(null);
                    configs.put(config.type(), config);
                } catch (IllegalAccessException e) {
                    throw new DaoException("Cannot access EntityConfig field: " + field.getName(), e);
                }
            }
        }
        return configs;
    }

    private static Map<String, QueryParam> buildQueryParams(String[] names, Class<?>[] types) {
        if (names.length == 0) return Map.of();
        var map = new HashMap<String, QueryParam>(names.length);
        for (int i = 0; i < names.length; i++) {
            map.put(names[i], new QueryParam(names[i], i, types[i]));
        }
        return Map.copyOf(map);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object[] extractBeanArgs(BeanMeta meta, Map<String, QueryParam> paramDefs, Object bean) {
        Object[] args = new Object[paramDefs.size()];
        if (bean != null) {
            for (var entry : paramDefs.entrySet()) {
                args[entry.getValue().position] = meta.get(bean, entry.getKey());
            }
        }
        return args;
    }

    private static String[] getParamNames(Method method) {
        Parameter[] params = method.getParameters();
        String[] names = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            names[i] = params[i].getName();
        }
        return names;
    }

    // --- Return type resolution ---

    private enum ReturnKind { VOID, INT, ONE, OPTIONAL, LIST }

    private static ReturnKind resolveReturnKind(Method method) {
        Class<?> rt = method.getReturnType();
        if (rt == void.class) return ReturnKind.VOID;
        if (rt == int.class || rt == Integer.class) return ReturnKind.INT;
        if (rt == Optional.class) return ReturnKind.OPTIONAL;
        if (rt == List.class) return ReturnKind.LIST;
        return ReturnKind.ONE;
    }

    private static Class<?> resolveElementType(Method method) {
        Class<?> rt = method.getReturnType();
        if (rt == Optional.class || rt == List.class) {
            Type genericReturn = method.getGenericReturnType();
            if (genericReturn instanceof ParameterizedType pt) {
                Type arg = pt.getActualTypeArguments()[0];
                if (arg instanceof Class<?> c) return c;
                if (arg instanceof ParameterizedType pt2) return (Class<?>) pt2.getRawType();
            }
        }
        return rt;
    }

    @FunctionalInterface
    interface MethodExecutor {
        Object execute(Object[] args);
    }

    @FunctionalInterface
    private interface ArgsMapper {
        Object[] map(Object[] args);
    }
}
