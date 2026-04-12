package eu.aston.beanmeta;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central registry for {@link BeanMeta} instances.
 * Tries ServiceLoader first (compile-time generated), falls back to reflection.
 */
public final class BeanMetaRegistry {

    private static final Map<Class<?>, BeanMeta<?>> CACHE = new ConcurrentHashMap<>();
    private static final Map<Class<?>, BeanMeta<?>> COMPILED = new ConcurrentHashMap<>();
    private static volatile boolean serviceLoaderInitialized = false;

    private BeanMetaRegistry() {}

    @SuppressWarnings("unchecked")
    public static <T> BeanMeta<T> forClass(Class<T> clazz) {
        BeanMeta<?> meta = CACHE.get(clazz);
        if (meta != null) return (BeanMeta<T>) meta;

        ensureServiceLoaderInitialized();

        meta = COMPILED.get(clazz);
        if (meta != null) {
            CACHE.put(clazz, meta);
            return (BeanMeta<T>) meta;
        }

        // Reflection fallback
        var reflective = new ReflectiveBeanMeta<>(clazz);
        CACHE.put(clazz, reflective);
        return reflective;
    }

    public static boolean hasCompiledMeta(Class<?> clazz) {
        ensureServiceLoaderInitialized();
        return COMPILED.containsKey(clazz);
    }

    private static void ensureServiceLoaderInitialized() {
        if (serviceLoaderInitialized) return;
        synchronized (BeanMetaRegistry.class) {
            if (serviceLoaderInitialized) return;
            ServiceLoader<BeanMeta> loader = ServiceLoader.load(BeanMeta.class);
            for (BeanMeta<?> meta : loader) {
                COMPILED.put(meta.type(), meta);
            }
            serviceLoaderInitialized = true;
        }
    }

    /** Visible for testing - resets internal state. */
    static void reset() {
        synchronized (BeanMetaRegistry.class) {
            CACHE.clear();
            COMPILED.clear();
            serviceLoaderInitialized = false;
        }
    }
}
