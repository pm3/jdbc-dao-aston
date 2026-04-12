package eu.aston.dao.impl;

import eu.aston.dao.ICondition;
import eu.aston.dao.Spread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pre-parsed SQL template. Parsing (regex) happens once per unique template string,
 * subsequent calls only assemble SQL from cached fragments.
 */
public final class SqlTemplate {

    private static final Pattern NAMED_PARAM = Pattern.compile("(?<!:):(\\w+)");
    private static final Pattern OPTIONAL_BLOCK = Pattern.compile("/\\*\\*(.*?)\\*\\*/", Pattern.DOTALL);

    /** Pre-built "?,?,?..." strings for Spread expansion (index = number of values). */
    private static final String[] SPREAD_PLACEHOLDERS = new String[33];
    static {
        var sb = new StringBuilder();
        SPREAD_PLACEHOLDERS[0] = "";
        for (int i = 1; i < SPREAD_PLACEHOLDERS.length; i++) {
            if (i > 1) sb.append(',');
            sb.append('?');
            SPREAD_PLACEHOLDERS[i] = sb.toString();
        }
    }

    private static final ConcurrentHashMap<String, SqlTemplate> cache = new ConcurrentHashMap<>();

    // --- Fragment types ---

    sealed interface Fragment {}
    record TextFragment(String text) implements Fragment {}
    record ParamFragment(String name) implements Fragment {}
    record OptionalBlockFragment(List<Fragment> parts, List<String> paramNames) implements Fragment {}

    // --- Instance state (immutable, created once per template) ---

    private final List<Fragment> fragments;
    /** For simple templates (no optional blocks): pre-built SQL with ? placeholders. */
    private final String simpleSql;
    /** For simple templates: ordered param names matching the ? positions. */
    private final List<String> simpleParamNames;

    private SqlTemplate(List<Fragment> fragments, String simpleSql, List<String> simpleParamNames) {
        this.fragments = fragments;
        this.simpleSql = simpleSql;
        this.simpleParamNames = simpleParamNames;
    }

    // --- Public API ---

    record ParsedSql(String sql, List<Object> params, JdbcBinder.ParamSetter[] setters) {}

    /** Get or create a cached SqlTemplate for the given template string. */
    public static SqlTemplate of(String template) {
        return cache.computeIfAbsent(template, SqlTemplate::parse);
    }

    /** Assemble final SQL from QueryParam definitions + args array. Setters lazily pre-resolved. */
    public ParsedSql process(Map<String, QueryParam> paramDefs, Object[] args) {
        if (paramDefs == null || paramDefs.isEmpty()) {
            if (simpleSql != null) return new ParsedSql(simpleSql, List.of(), null);
            var sb = new StringBuilder();
            var positionalParams = new ArrayList<Object>();
            assembleFragments(fragments, Map.of(), sb, positionalParams);
            return new ParsedSql(sb.toString(), positionalParams, null);
        }
        // Fast path: simple template without Spread/ICondition values
        if (simpleSql != null && !hasSpecialArgs(paramDefs, args)) {
            int size = simpleParamNames.size();
            var positionalParams = new ArrayList<Object>(size);
            var setters = new JdbcBinder.ParamSetter[size];
            for (int i = 0; i < size; i++) {
                QueryParam pp = paramDefs.get(simpleParamNames.get(i));
                positionalParams.add(args[pp.position]);
                setters[i] = pp.setter();
            }
            return new ParsedSql(simpleSql, positionalParams, setters);
        }
        // Slow path: convert to Map<String, Object> for fragment assembly
        var namedParams = new java.util.HashMap<String, Object>(paramDefs.size());
        for (var entry : paramDefs.entrySet()) {
            namedParams.put(entry.getKey(), args[entry.getValue().position]);
        }
        var sb = new StringBuilder();
        var positionalParams = new ArrayList<Object>();
        assembleFragments(fragments, namedParams, sb, positionalParams);
        return new ParsedSql(sb.toString(), positionalParams, null);
    }

    private boolean hasSpecialArgs(Map<String, QueryParam> paramDefs, Object[] args) {
        for (String name : simpleParamNames) {
            QueryParam pp = paramDefs.get(name);
            if (pp != null) {
                Object value = args[pp.position];
                if (value instanceof Spread<?> || value instanceof ICondition) return true;
            }
        }
        return false;
    }

    // --- Parsing (runs once per template) ---

    private static SqlTemplate parse(String template) {
        var fragments = new ArrayList<Fragment>();
        boolean hasOptionalBlocks = false;
        var blockMatcher = OPTIONAL_BLOCK.matcher(template);
        int pos = 0;
        while (blockMatcher.find()) {
            hasOptionalBlocks = true;
            if (blockMatcher.start() > pos) {
                parseNamedParams(template.substring(pos, blockMatcher.start()), fragments);
            }
            String blockContent = blockMatcher.group(1);
            var blockParts = new ArrayList<Fragment>();
            var blockParamNames = new ArrayList<String>();
            var paramMatcher = NAMED_PARAM.matcher(blockContent);
            int bPos = 0;
            while (paramMatcher.find()) {
                if (paramMatcher.start() > bPos) {
                    blockParts.add(new TextFragment(blockContent.substring(bPos, paramMatcher.start())));
                }
                String name = paramMatcher.group(1);
                blockParts.add(new ParamFragment(name));
                blockParamNames.add(name);
                bPos = paramMatcher.end();
            }
            if (bPos < blockContent.length()) {
                blockParts.add(new TextFragment(blockContent.substring(bPos)));
            }
            fragments.add(new OptionalBlockFragment(List.copyOf(blockParts), List.copyOf(blockParamNames)));
            pos = blockMatcher.end();
        }
        if (pos < template.length()) {
            parseNamedParams(template.substring(pos), fragments);
        }
        var immutableFragments = List.copyOf(fragments);

        // Pre-build simple SQL for templates without optional blocks
        if (!hasOptionalBlocks) {
            var sb = new StringBuilder();
            var paramNames = new ArrayList<String>();
            for (Fragment f : immutableFragments) {
                if (f instanceof TextFragment t) {
                    sb.append(t.text());
                } else if (f instanceof ParamFragment p) {
                    sb.append('?');
                    paramNames.add(p.name());
                }
            }
            return new SqlTemplate(immutableFragments, sb.toString(), List.copyOf(paramNames));
        }

        return new SqlTemplate(immutableFragments, null, null);
    }

    private static void parseNamedParams(String text, List<Fragment> out) {
        var matcher = NAMED_PARAM.matcher(text);
        int pos = 0;
        while (matcher.find()) {
            if (matcher.start() > pos) {
                out.add(new TextFragment(text.substring(pos, matcher.start())));
            }
            out.add(new ParamFragment(matcher.group(1)));
            pos = matcher.end();
        }
        if (pos < text.length()) {
            out.add(new TextFragment(text.substring(pos)));
        }
    }

    // --- Assembly (runs per call, but no regex) ---

    private static void assembleFragments(List<Fragment> fragments, Map<String, Object> params,
                                           StringBuilder sb, List<Object> positionalParams) {
        for (Fragment f : fragments) {
            if (f instanceof TextFragment t) {
                sb.append(t.text());
            } else if (f instanceof ParamFragment p) {
                appendParam(p.name(), params, sb, positionalParams);
            } else if (f instanceof OptionalBlockFragment ob) {
                boolean hasNull = false;
                for (String name : ob.paramNames()) {
                    if (params.get(name) == null) { hasNull = true; break; }
                }
                if (!hasNull) {
                    assembleFragments(ob.parts(), params, sb, positionalParams);
                }
            }
        }
    }

    private static void appendParam(String paramName, Map<String, Object> params,
                                     StringBuilder sb, List<Object> positionalParams) {
        Object value = params.get(paramName);
        if (value instanceof Spread<?> spread) {
            int size = spread.values().size();
            sb.append(size < SPREAD_PLACEHOLDERS.length
                    ? SPREAD_PLACEHOLDERS[size]
                    : String.join(",", Collections.nCopies(size, "?")));
            positionalParams.addAll(spread.values());
        } else if (value instanceof ICondition cond) {
            sb.append(cond.sql().isEmpty() ? "1=1" : cond.sql());
            positionalParams.addAll(cond.params());
        } else {
            sb.append('?');
            positionalParams.add(value);
        }
    }
}
