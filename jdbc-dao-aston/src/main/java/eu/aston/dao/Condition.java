package eu.aston.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Builder for dynamic SQL WHERE conditions. Null values are silently skipped — every value-based condition is
 * automatically optional.
 */
public final class Condition {

    private static final ICondition EMPTY = (sql, params) -> false;

    private Condition() {
    }

    public static ICondition eq(String column, Object value) {
        return value == null ? EMPTY : new BinaryCondition(column, " = ?", value);
    }

    public static ICondition ne(String column, Object value) {
        return value == null ? EMPTY : new BinaryCondition(column, " != ?", value);
    }

    public static ICondition lt(String column, Object value) {
        return value == null ? EMPTY : new BinaryCondition(column, " < ?", value);
    }

    public static ICondition le(String column, Object value) {
        return value == null ? EMPTY : new BinaryCondition(column, " <= ?", value);
    }

    public static ICondition gt(String column, Object value) {
        return value == null ? EMPTY : new BinaryCondition(column, " > ?", value);
    }

    public static ICondition ge(String column, Object value) {
        return value == null ? EMPTY : new BinaryCondition(column, " >= ?", value);
    }

    public static ICondition like(String column, Object value) {
        return value == null ? EMPTY : new BinaryCondition(column, " LIKE ?", value);
    }

    public static ICondition in(String column, Collection<?> values) {
        if (values == null || values.isEmpty())
            return EMPTY;
        return new InCondition(column, new ArrayList<>(values));
    }

    public static ICondition isNull(String column) {
        return new RawCondition(column + " IS NULL", List.of());
    }

    public static ICondition isNotNull(String column) {
        return new RawCondition(column + " IS NOT NULL", List.of());
    }

    public static ICondition between(String column, Object from, Object to) {
        if (from == null && to == null)
            return EMPTY;
        if (from == null)
            return new BinaryCondition(column, " < ?", to);
        if (to == null)
            return new BinaryCondition(column, " > ?", from);
        return new BetweenCondition(column, from, to);
    }

    public static ICondition raw(String sql, Object... params) {
        if (sql == null || sql.isEmpty())
            return EMPTY;
        return new RawCondition(sql, params == null ? List.of() : List.of(params));
    }

    public static ICondition and(ICondition... conditions) {
        return composite(" AND ", conditions);
    }

    public static ICondition or(ICondition... conditions) {
        return composite(" OR ", conditions);
    }

    public static ICondition not(ICondition condition) {
        return condition == null ? EMPTY : new NotCondition(condition);
    }

    private static ICondition composite(String separator, ICondition[] conditions) {
        if (conditions == null || conditions.length == 0)
            return EMPTY;
        if (conditions.length == 1)
            return conditions[0] == null ? EMPTY : conditions[0];
        return new CompositeCondition(separator, conditions);
    }

    private record BinaryCondition(String column, String op, Object value) implements ICondition {
        @Override
        public boolean build(StringBuilder sql, List<Object> params) {
            sql.append(column).append(op);
            params.add(value);
            return true;
        }
    }

    private record InCondition(String column, List<Object> values) implements ICondition {
        @Override
        public boolean build(StringBuilder sql, List<Object> params) {
            sql.append(column).append(" IN (");
            int n = values.size();
            for (int i = 0; i < n; i++) {
                if (i > 0)
                    sql.append(',');
                sql.append('?');
            }
            sql.append(')');
            params.addAll(values);
            return true;
        }
    }

    private record BetweenCondition(String column, Object from, Object to) implements ICondition {
        @Override
        public boolean build(StringBuilder sql, List<Object> params) {
            sql.append(column).append(" BETWEEN ? AND ?");
            params.add(from);
            params.add(to);
            return true;
        }
    }

    private record RawCondition(String sql, List<Object> values) implements ICondition {
        @Override
        public boolean build(StringBuilder out, List<Object> params) {
            out.append(sql);
            params.addAll(values);
            return true;
        }
    }

    private record CompositeCondition(String separator, ICondition[] parts) implements ICondition {
        @Override
        public boolean build(StringBuilder sql, List<Object> params) {
            int startLen = sql.length();
            sql.append('(');
            boolean any = false;
            for (ICondition c : parts) {
                if (c == null)
                    continue;
                int beforeLen = sql.length();
                if (any)
                    sql.append(separator);
                if (c.build(sql, params)) {
                    any = true;
                } else {
                    sql.setLength(beforeLen);
                }
            }
            if (!any) {
                sql.setLength(startLen);
                return false;
            }
            sql.append(')');
            return true;
        }
    }

    private record NotCondition(ICondition inner) implements ICondition {
        @Override
        public boolean build(StringBuilder sql, List<Object> params) {
            int startLen = sql.length();
            sql.append("NOT (");
            if (!inner.build(sql, params)) {
                sql.setLength(startLen);
                return false;
            }
            sql.append(')');
            return true;
        }
    }
}
