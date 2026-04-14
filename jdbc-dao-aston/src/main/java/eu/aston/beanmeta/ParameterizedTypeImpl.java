package eu.aston.beanmeta;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Simple implementation of {@link ParameterizedType} for representing generic types.
 */
public final class ParameterizedTypeImpl implements ParameterizedType {

    private final Class<?> rawType;
    private final Type[] actualTypeArguments;

    public ParameterizedTypeImpl(Class<?> rawType, Type[] actualTypeArguments) {
        this.rawType = Objects.requireNonNull(rawType);
        this.actualTypeArguments = actualTypeArguments.clone();
    }

    @Override
    public Type[] getActualTypeArguments() {
        return actualTypeArguments.clone();
    }

    @Override
    public Type getRawType() {
        return rawType;
    }

    @Override
    public Type getOwnerType() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ParameterizedType that))
            return false;
        return rawType.equals(that.getRawType()) && Arrays.equals(actualTypeArguments, that.getActualTypeArguments())
                && that.getOwnerType() == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawType) ^ Arrays.hashCode(actualTypeArguments);
    }

    @Override
    public String toString() {
        if (actualTypeArguments.length == 0)
            return rawType.getTypeName();
        var sj = new StringJoiner(", ", rawType.getTypeName() + "<", ">");
        for (Type t : actualTypeArguments)
            sj.add(t.getTypeName());
        return sj.toString();
    }
}
