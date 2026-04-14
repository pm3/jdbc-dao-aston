package eu.aston.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.dao.DaoException;
import eu.aston.dao.NoRowsException;
import eu.aston.dao.TooManyRowsException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Internal SQL execution helper — template processing, parameter binding, result reading.
 */
public final class SqlHelper {

    private SqlHelper() {
    }

    // --- Query execution ---

    static <T> T queryOne(DataSource ds, ObjectMapper om, Class<T> type, String sqlTemplate,
            Map<String, QueryParam> paramDefs, Object[] args) {
        List<T> results = queryList(ds, om, type, sqlTemplate, paramDefs, args);
        if (results.isEmpty())
            throw new NoRowsException("Expected 1 row, got 0");
        if (results.size() > 1)
            throw new TooManyRowsException("Expected 1 row, got " + results.size());
        return results.get(0);
    }

    static <T> Optional<T> queryOptional(DataSource ds, ObjectMapper om, Class<T> type, String sqlTemplate,
            Map<String, QueryParam> paramDefs, Object[] args) {
        List<T> results = queryList(ds, om, type, sqlTemplate, paramDefs, args);
        if (results.isEmpty())
            return Optional.empty();
        if (results.size() > 1)
            throw new TooManyRowsException("Expected 0-1 rows, got " + results.size());
        return Optional.of(results.get(0));
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> queryList(DataSource ds, ObjectMapper om, Class<T> type, String sqlTemplate,
            Map<String, QueryParam> paramDefs, Object[] args) {
        var parsed = SqlTemplate.of(sqlTemplate).process(paramDefs, args);
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed, om);
            if (JdbcBinder.isScalar(type)) {
                return readScalarResults(ps, type);
            }
            return BeanReader.forClass(type).readBeanResults(ps, om);
        } catch (SQLException e) {
            throw new DaoException("Query failed: " + parsed.sql(), e);
        }
    }

    // --- Execute (no result) ---

    static void execute(DataSource ds, ObjectMapper om, String sqlTemplate, Map<String, QueryParam> paramDefs,
            Object[] args) {
        var parsed = SqlTemplate.of(sqlTemplate).process(paramDefs, args);
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed, om);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Execute failed: " + parsed.sql(), e);
        }
    }

    static int executeUpdate(DataSource ds, ObjectMapper om, String sqlTemplate, Map<String, QueryParam> paramDefs,
            Object[] args) {
        var parsed = SqlTemplate.of(sqlTemplate).process(paramDefs, args);
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(parsed.sql())) {
            bindParams(ps, parsed, om);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("ExecuteUpdate failed: " + parsed.sql(), e);
        }
    }

    // --- Shared helpers ---

    @SuppressWarnings("unchecked")
    private static <T> List<T> readScalarResults(PreparedStatement ps, Class<T> type) throws SQLException {
        JdbcBinder.ColumnReader reader = JdbcBinder.readerFor(type);
        try (ResultSet rs = ps.executeQuery()) {
            var results = new ArrayList<T>();
            while (rs.next()) {
                results.add((T) reader.read(rs, 1));
            }
            return results;
        }
    }

    private static void bindParams(PreparedStatement ps, SqlTemplate.ParsedSql parsed, ObjectMapper om)
            throws SQLException {
        var params = parsed.params();
        var setters = parsed.setters();
        for (int i = 0; i < params.size(); i++) {
            JdbcBinder.ParamSetter setter = setters.get(i);
            if (setter != null) {
                JdbcBinder.setParam(ps, i + 1, params.get(i), setter, om);
            } else {
                JdbcBinder.setParam(ps, i + 1, params.get(i), om);
            }
        }
    }
}
