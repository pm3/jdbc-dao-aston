package eu.aston.dao;

import java.util.List;

/**
 * Dynamic SQL condition fragment with positional parameters. Implementations append their SQL and parameters directly
 * into the caller's buffers to avoid intermediate string and list allocations.
 */
public interface ICondition {

    /**
     * Append SQL fragment and positional parameters into the given buffers. Returns {@code true} if anything was
     * emitted, {@code false} if the condition contributed nothing (in which case the buffers are left untouched).
     */
    boolean build(StringBuilder sql, List<Object> params);
}
