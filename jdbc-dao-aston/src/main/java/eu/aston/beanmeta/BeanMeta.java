package eu.aston.beanmeta;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Unified introspection API for beans and records.
 *
 * @param <T>
 *            the bean/record type
 */
public interface BeanMeta<T> {

    /** The bean/record class this meta describes. */
    Class<T> type();

    /** Ordered list of property names. */
    List<String> names();

    /** Ordered list of property types, aligned with names(). */
    List<Class<?>> types();

    /** Ordered list of generic property types, aligned with names(). */
    List<Type> genericTypes();

    /** Read a property value by name. */
    Object get(T instance, String name);

    /** Create a new instance. Values must be in names() order. */
    T create(Object... values);
}
