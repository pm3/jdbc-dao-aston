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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reflection-based proxy InvocationHandler for DAO interfaces.
 * Fallback when no compile-time generated implementation is found.
 */
public final class DaoProxy implements InvocationHandler {

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;
    private final Map<Class<?>, EntityConfig<?>> entityConfigs;
    private final Map<Method, MethodExecutor> executors = new ConcurrentHashMap<>();

    public DaoProxy(Class<?> daoInterface, DataSource dataSource, ObjectMapper objectMapper) {
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
        this.entityConfigs = scanEntityConfigs(daoInterface);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }
        var executor = executors.computeIfAbsent(method, this::buildExecutor);
        return executor.execute(args);
    }

    private MethodExecutor buildExecutor(Method method) {
        Query queryAnn = method.getAnnotation(Query.class);
        if (queryAnn != null) {
            return buildQueryExecutor(method, queryAnn.value());
        }
        return buildEntityExecutor(method);
    }

    // --- @Query methods ---

    private MethodExecutor buildQueryExecutor(Method method, String sqlTemplate) {
        String[] paramNames = getParamNames(method);
        ReturnKind returnKind = resolveReturnKind(method);
        Class<?> elementType = resolveElementType(method);
        Type genericElementType = resolveGenericElementType(method);

        return args -> {
            var params = buildParamMap(paramNames, args);
            return switch (returnKind) {
                case VOID -> { SqlHelper.execute(dataSource, objectMapper, sqlTemplate, params); yield null; }
                case INT -> SqlHelper.executeUpdate(dataSource, objectMapper, sqlTemplate, params);
                case ONE -> SqlHelper.queryOne(dataSource, objectMapper, elementType, genericElementType, sqlTemplate, params);
                case OPTIONAL -> SqlHelper.queryOptional(dataSource, objectMapper, elementType, genericElementType, sqlTemplate, params);
                case LIST -> SqlHelper.queryList(dataSource, objectMapper, elementType, genericElementType, sqlTemplate, params);
            };
        };
    }

    // --- Entity convention methods ---

    private MethodExecutor buildEntityExecutor(Method method) {
        String name = method.getName();
        if (name.startsWith("load")) return buildLoadExecutor(method);
        if (name.startsWith("insert")) return buildInsertExecutor(method);
        if (name.startsWith("update")) return buildUpdateExecutor(method);
        if (name.startsWith("save")) return buildSaveExecutor(method);
        if (name.startsWith("delete")) return buildDeleteExecutor(method);
        throw new DaoException("Cannot resolve method: " + method.getName()
                + " — must have @Query or start with load/insert/update/save/delete");
    }

    private MethodExecutor buildLoadExecutor(Method method) {
        Class<?> returnType = method.getReturnType();
        EntityConfig<?> config = findConfig(returnType);
        return args -> SqlHelper.entityLoad(dataSource, objectMapper, config, args[0]);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private MethodExecutor buildInsertExecutor(Method method) {
        Class<?> paramType = method.getParameterTypes()[0];
        EntityConfig config = findConfig(paramType);
        return args -> { SqlHelper.entityInsert(dataSource, objectMapper, config, args[0]); return null; };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private MethodExecutor buildUpdateExecutor(Method method) {
        Class<?> paramType = method.getParameterTypes()[0];
        EntityConfig config = findConfig(paramType);
        return args -> { SqlHelper.entityUpdate(dataSource, objectMapper, config, args[0]); return null; };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private MethodExecutor buildSaveExecutor(Method method) {
        Class<?> paramType = method.getParameterTypes()[0];
        EntityConfig config = findConfig(paramType);
        return args -> { SqlHelper.entitySave(dataSource, objectMapper, config, args[0]); return null; };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private MethodExecutor buildDeleteExecutor(Method method) {
        if (method.getParameterTypes().length == 1) {
            Class<?> paramType = method.getParameterTypes()[0];
            EntityConfig config = entityConfigs.get(paramType);
            if (config != null) {
                return args -> { SqlHelper.entityDelete(dataSource, objectMapper, config, args[0]); return null; };
            }
            // parameter is a PK value — find the only config or match by method name suffix
            if (entityConfigs.size() == 1) {
                EntityConfig singleConfig = entityConfigs.values().iterator().next();
                return args -> { SqlHelper.entityDelete(dataSource, objectMapper, singleConfig, args[0]); return null; };
            }
        }
        throw new DaoException("Cannot resolve delete entity for method: " + method.getName());
    }

    // --- Helpers ---

    private EntityConfig<?> findConfig(Class<?> beanType) {
        EntityConfig<?> config = entityConfigs.get(beanType);
        if (config == null) {
            throw new DaoException("No EntityConfig found for type: " + beanType.getName());
        }
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

    private static String[] getParamNames(Method method) {
        Parameter[] params = method.getParameters();
        String[] names = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            names[i] = params[i].getName();
        }
        return names;
    }

    private static Map<String, Object> buildParamMap(String[] names, Object[] args) {
        var map = new HashMap<String, Object>();
        if (args != null) {
            for (int i = 0; i < names.length; i++) {
                map.put(names[i], args[i]);
            }
        }
        return map;
    }

    private static String getSimpleName(Method method) {
        return method.getName();
    }

    // --- Return type resolution ---

    enum ReturnKind { VOID, INT, ONE, OPTIONAL, LIST }

    static ReturnKind resolveReturnKind(Method method) {
        Class<?> returnType = method.getReturnType();
        if (returnType == void.class) return ReturnKind.VOID;
        if (returnType == int.class || returnType == Integer.class) return ReturnKind.INT;
        if (returnType == Optional.class) return ReturnKind.OPTIONAL;
        if (returnType == List.class) return ReturnKind.LIST;
        return ReturnKind.ONE;
    }

    static Class<?> resolveElementType(Method method) {
        Class<?> returnType = method.getReturnType();
        if (returnType == Optional.class || returnType == List.class) {
            Type genericReturn = method.getGenericReturnType();
            if (genericReturn instanceof ParameterizedType pt) {
                Type arg = pt.getActualTypeArguments()[0];
                if (arg instanceof Class<?> c) return c;
                if (arg instanceof ParameterizedType pt2) return (Class<?>) pt2.getRawType();
            }
        }
        return returnType;
    }

    static Type resolveGenericElementType(Method method) {
        Class<?> returnType = method.getReturnType();
        if (returnType == Optional.class || returnType == List.class) {
            Type genericReturn = method.getGenericReturnType();
            if (genericReturn instanceof ParameterizedType pt) {
                return pt.getActualTypeArguments()[0];
            }
        }
        return returnType;
    }

    @FunctionalInterface
    interface MethodExecutor {
        Object execute(Object[] args);
    }
}
