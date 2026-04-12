package eu.aston.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Builder for dynamic SQL WHERE conditions.
 * Null values are silently skipped — every value-based condition is automatically optional.
 */
public final class Condition {

    private Condition() {}

    public static ICondition eq(String column, Object value) {
        if (value == null) return empty();
        return simple(column + " = ?", value);
    }

    public static ICondition ne(String column, Object value) {
        if (value == null) return empty();
        return simple(column + " != ?", value);
    }

    public static ICondition lt(String column, Object value) {
        if (value == null) return empty();
        return simple(column + " < ?", value);
    }

    public static ICondition le(String column, Object value) {
        if (value == null) return empty();
        return simple(column + " <= ?", value);
    }

    public static ICondition gt(String column, Object value) {
        if (value == null) return empty();
        return simple(column + " > ?", value);
    }

    public static ICondition ge(String column, Object value) {
        if (value == null) return empty();
        return simple(column + " >= ?", value);
    }

    public static ICondition like(String column, Object value) {
        if (value == null) return empty();
        return simple(column + " LIKE ?", value);
    }

    public static ICondition in(String column, Collection<?> values) {
        if (values == null || values.isEmpty()) return empty();
        var placeholders = String.join(",", Collections.nCopies(values.size(), "?"));
        return new SimpleCondition(column + " IN (" + placeholders + ")", new ArrayList<>(values));
    }

    public static ICondition isNull(String column) {
        return new SimpleCondition(column + " IS NULL", List.of());
    }

    public static ICondition isNotNull(String column) {
        return new SimpleCondition(column + " IS NOT NULL", List.of());
    }

    public static ICondition between(String column, Object from, Object to) {
        if (from == null || to == null) return empty();
        return new SimpleCondition(column + " BETWEEN ? AND ?", List.of(from, to));
    }

    public static ICondition raw(String sql, Object... params) {
        return new SimpleCondition(sql, params == null ? List.of() : List.of(params));
    }

    public static ICondition and(ICondition... conditions) {
        return composite("AND", conditions);
    }

    public static ICondition or(ICondition... conditions) {
        return composite("OR", conditions);
    }

    public static ICondition not(ICondition condition) {
        if (condition == null || condition.sql().isEmpty()) return empty();
        var params = new ArrayList<>(condition.params());
        return new SimpleCondition("NOT (" + condition.sql() + ")", params);
    }

    private static ICondition composite(String op, ICondition[] conditions) {
        var parts = new ArrayList<ICondition>();
        for (ICondition c : conditions) {
            if (c != null && !c.sql().isEmpty()) parts.add(c);
        }
        if (parts.isEmpty()) return empty();
        if (parts.size() == 1) return parts.get(0);
        var sj = new StringJoiner(" " + op + " ", "(", ")");
        var allParams = new ArrayList<Object>();
        for (ICondition c : parts) {
            sj.add(c.sql());
            allParams.addAll(c.params());
        }
        return new SimpleCondition(sj.toString(), allParams);
    }

    private static ICondition simple(String sql, Object value) {
        return new SimpleCondition(sql, List.of(value));
    }

    private static ICondition empty() {
        return new SimpleCondition("", List.of());
    }

    private record SimpleCondition(String sql, List<Object> params) implements ICondition {}
}
