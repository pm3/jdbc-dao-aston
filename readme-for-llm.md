# jdbc-dao-aston

Lightweight Java JDBC DAO library. No ORM — clean SQL, named parameters, optional WHERE blocks. DAO interfaces defined declaratively, implementation generated at compile time (annotation processor) with reflection fallback.

## Core concepts

1. **@DaoApi interface** — declare DAO methods, register `EntityConfig` static fields
2. **EntityConfig** — maps entity class to table: `EntityConfig.of(User.class, "users").pk("id").createdAt("createdat").updatedAt("updatedat").build()`
3. **DaoRegistry** — obtain implementation: `DaoRegistry.forClass(UserDao.class, dataSource)` or `DaoRegistry.forClass(UserDao.class, dataSource, objectMapper)` for JSON columns
4. **@GenerateMeta** on entity classes — optional compile-time bean introspection (GraalVM-friendly)

## Column matching

Case-insensitive property-to-column: `lower(propertyName) = lower(columnName)`. No camelCase-to-snake_case conversion.

## Convention methods (no @Query)

Prefix determines operation, entity resolved by return/param type against EntityConfig fields:

- `load*` — SELECT by PK, returns T (throws NoRowsException if missing)
- `insert*` — INSERT
- `update*` — UPDATE by PK
- `save*` — PK empty=INSERT, else UPDATE
- `delete*` — DELETE by PK (accepts entity or PK value)

## @Query methods

Custom SQL with `:name` named parameters mapped from method param names.

**Return types:** `T` (exactly 1 row), `Optional<T>` (0-1 row), `List<T>` (all rows), `void` (execute), `int` (affected count). Scalars supported for single-column selects.

**Optional WHERE blocks:** `/** AND col=:param **/` — removed when param is null.

**Spread (IN clause):** `Spread<T>` param expands into `IN (?,?,?)`. Use `Spread.of(values)`.

**Bean param expansion:** single bean/record param auto-expands properties as named params.

**ICondition (dynamic WHERE):** `ICondition` param with `:where` placeholder. Build with `Condition.and/or/not/eq/ne/lt/le/gt/ge/like/in/isNull/isNotNull/between/raw`. Null values silently skip the condition.

## JSON columns

Auto-serialized based on Java type (beans, records, List, Set, Map = JSON; scalars = no). Requires ObjectMapper in DaoRegistry.

## Exceptions

`NoRowsException`, `TooManyRowsException`, `DaoException` (base). Package: `eu.aston.dao`.

## Requirements

Java 17+, JDBC driver, Jackson ObjectMapper (optional, for JSON).
