package eu.aston.dao;

import java.util.List;

/**
 * Dynamic SQL condition fragment with positional parameters.
 */
public interface ICondition {

    /** SQL fragment (e.g. "active = ? AND name LIKE ?"). */
    String sql();

    /** Positional parameter values matching ? placeholders in sql(). */
    List<Object> params();
}
