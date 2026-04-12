package eu.aston.beanmeta;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reflection-based implementation of {@link BeanMeta}.
 * Supports records and JavaBeans (getter/setter or getter + all-args constructor).
 */
final class ReflectiveBeanMeta<T> implements BeanMeta<T> {

    private final Class<T> type;
    private final List<String> names;
    private final List<Class<?>> types;
    private final List<Type> genericTypes;
    private final Map<String, MethodHandle> getters;
    private final MethodHandle constructor;
    private final Map<String, MethodHandle> setters; // null for records / immutable beans
    private final boolean useSetters;

    ReflectiveBeanMeta(Class<T> type) {
        this.type = type;
        var lookup = MethodHandles.lookup();

        if (type.isRecord()) {
            RecordComponent[] components = type.getRecordComponents();
            var nameList = new ArrayList<String>(components.length);
            var typeList = new ArrayList<Class<?>>(components.length);
            var genericList = new ArrayList<Type>(components.length);
            var getterMap = new LinkedHashMap<String, MethodHandle>(components.length);
            var ctorParamTypes = new Class<?>[components.length];

            for (int i = 0; i < components.length; i++) {
                RecordComponent rc = components[i];
                nameList.add(rc.getName());
                typeList.add(rc.getType());
                genericList.add(rc.getGenericType());
                ctorParamTypes[i] = rc.getType();
                try {
                    getterMap.put(rc.getName(), lookup.unreflect(rc.getAccessor()));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Cannot access record accessor: " + rc.getName(), e);
                }
            }

            this.names = Collections.unmodifiableList(nameList);
            this.types = Collections.unmodifiableList(typeList);
            this.genericTypes = Collections.unmodifiableList(genericList);
            this.getters = Collections.unmodifiableMap(getterMap);
            this.setters = null;
            this.useSetters = false;

            try {
                Constructor<T> ctor = type.getDeclaredConstructor(ctorParamTypes);
                ctor.setAccessible(true);
                this.constructor = lookup.unreflectConstructor(ctor);
            } catch (Exception e) {
                throw new RuntimeException("Cannot access canonical constructor of record: " + type.getName(), e);
            }
        } else {
            // JavaBean: discover getters via get*/is* methods
            var nameList = new ArrayList<String>();
            var typeList = new ArrayList<Class<?>>();
            var genericList = new ArrayList<Type>();
            var getterMap = new LinkedHashMap<String, MethodHandle>();
            var setterMap = new LinkedHashMap<String, MethodHandle>();

            for (Method m : type.getMethods()) {
                if (m.getDeclaringClass() == Object.class) continue;
                if (m.getParameterCount() != 0) continue;

                String name = null;
                if (m.getName().startsWith("get") && m.getName().length() > 3) {
                    name = Character.toLowerCase(m.getName().charAt(3)) + m.getName().substring(4);
                } else if (m.getName().startsWith("is") && m.getName().length() > 2
                        && (m.getReturnType() == boolean.class || m.getReturnType() == Boolean.class)) {
                    name = Character.toLowerCase(m.getName().charAt(2)) + m.getName().substring(3);
                }

                if (name == null || "class".equals(name)) continue;

                nameList.add(name);
                typeList.add(m.getReturnType());
                genericList.add(m.getGenericReturnType());
                try {
                    getterMap.put(name, lookup.unreflect(m));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Cannot access getter: " + m.getName(), e);
                }

                // try to find setter
                String setterName = "set" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
                try {
                    Method setter = type.getMethod(setterName, m.getReturnType());
                    setterMap.put(name, lookup.unreflect(setter));
                } catch (NoSuchMethodException | IllegalAccessException ignored) {
                    // no setter
                }
            }

            this.names = Collections.unmodifiableList(nameList);
            this.types = Collections.unmodifiableList(typeList);
            this.genericTypes = Collections.unmodifiableList(genericList);
            this.getters = Collections.unmodifiableMap(getterMap);

            // Decide construction strategy: setters or all-args constructor
            if (setterMap.size() == nameList.size()) {
                // All properties have setters - use no-arg constructor + setters
                this.setters = Collections.unmodifiableMap(setterMap);
                this.useSetters = true;
                try {
                    Constructor<T> ctor = type.getDeclaredConstructor();
                    ctor.setAccessible(true);
                    this.constructor = lookup.unreflectConstructor(ctor);
                } catch (Exception e) {
                    throw new RuntimeException("Cannot access no-arg constructor of: " + type.getName(), e);
                }
            } else {
                // Try all-args constructor
                this.setters = null;
                this.useSetters = false;
                Class<?>[] paramTypes = typeList.toArray(new Class<?>[0]);
                try {
                    Constructor<T> ctor = type.getDeclaredConstructor(paramTypes);
                    ctor.setAccessible(true);
                    this.constructor = lookup.unreflectConstructor(ctor);
                } catch (Exception e) {
                    throw new RuntimeException("Cannot find suitable constructor for: " + type.getName(), e);
                }
            }
        }
    }

    @Override
    public Class<T> type() {
        return type;
    }

    @Override
    public List<String> names() {
        return names;
    }

    @Override
    public List<Class<?>> types() {
        return types;
    }

    @Override
    public List<Type> genericTypes() {
        return genericTypes;
    }

    @Override
    public Object get(T instance, String name) {
        MethodHandle getter = getters.get(name);
        if (getter == null) {
            throw new IllegalArgumentException("Unknown property: " + name);
        }
        try {
            return getter.invoke(instance);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to get property: " + name, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T create(Object... values) {
        try {
            if (useSetters) {
                T instance = (T) constructor.invoke();
                for (int i = 0; i < names.size(); i++) {
                    MethodHandle setter = setters.get(names.get(i));
                    setter.invoke(instance, values[i]);
                }
                return instance;
            } else {
                return (T) constructor.invokeWithArguments(values);
            }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to create instance of: " + type.getName(), e);
        }
    }
}
