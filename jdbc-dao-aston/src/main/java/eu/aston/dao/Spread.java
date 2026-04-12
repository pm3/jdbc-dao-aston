package eu.aston.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Wraps multiple values for expanding into IN clause placeholders.
 * Spread.of() throws if empty — unless used inside an optional block.
 */
public final class Spread<T> {

    private final List<T> values;

    private Spread(List<T> values) {
        this.values = values;
    }

    public List<T> values() {
        return values;
    }

    @SafeVarargs
    public static <T> Spread<T> of(T... values) {
        if (values == null || values.length == 0) {
            throw new DaoException("Spread.of() requires at least one value");
        }
        return new Spread<>(Collections.unmodifiableList(Arrays.asList(values)));
    }

    public static <T> Spread<T> of(Collection<T> values) {
        if (values == null || values.isEmpty()) {
            throw new DaoException("Spread.of() requires at least one value");
        }
        return new Spread<>(Collections.unmodifiableList(new ArrayList<>(values)));
    }
}
