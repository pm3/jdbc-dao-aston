package eu.aston.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.dao.impl.DaoProxy;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central registry for DAO instances.
 * Tries ServiceLoader first (compile-time generated), falls back to reflection proxy.
 */
public final class DaoRegistry {

    private static final Map<Class<?>, DaoProvider> PROVIDERS = new ConcurrentHashMap<>();
    private static volatile boolean serviceLoaderInitialized = false;

    private DaoRegistry() {}

    /**
     * Create a DAO instance without JSON support.
     */
    public static <T> T forClass(Class<T> daoInterface, DataSource dataSource) {
        return forClass(daoInterface, dataSource, null);
    }

    /**
     * Create a DAO instance with JSON support via ObjectMapper.
     */
    @SuppressWarnings("unchecked")
    public static <T> T forClass(Class<T> daoInterface, DataSource dataSource, ObjectMapper objectMapper) {
        ensureServiceLoaderInitialized();

        DaoProvider provider = PROVIDERS.get(daoInterface);
        if (provider != null) {
            return (T) provider.newInstance(dataSource, objectMapper);
        }

        // Reflection fallback
        var handler = new DaoProxy(daoInterface, dataSource, objectMapper);
        return (T) Proxy.newProxyInstance(
                daoInterface.getClassLoader(),
                new Class<?>[]{daoInterface},
                handler
        );
    }

    private static void ensureServiceLoaderInitialized() {
        if (serviceLoaderInitialized) return;
        synchronized (DaoRegistry.class) {
            if (serviceLoaderInitialized) return;
            ServiceLoader<DaoProvider> loader = ServiceLoader.load(DaoProvider.class);
            for (DaoProvider provider : loader) {
                PROVIDERS.put(provider.daoInterface(), provider);
            }
            serviceLoaderInitialized = true;
        }
    }

    /** Visible for testing — resets internal state. */
    static void reset() {
        synchronized (DaoRegistry.class) {
            PROVIDERS.clear();
            serviceLoaderInitialized = false;
        }
    }
}
