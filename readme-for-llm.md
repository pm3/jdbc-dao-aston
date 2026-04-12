# jdbc-dao-aston

Lightweight Java JDBC DAO library. No ORM — clean SQL, named parameters, optional WHERE blocks. DAO interfaces defined declaratively, implementation generated at compile time (annotation processor) with reflection fallback.

## Core concepts

1. **@DaoApi interface** — declare DAO methods, register `EntityConfig` static fields
2. **EntityConfig** — maps entity class to table: `EntityConfig.of(User.class, "users").pk("id").createdAt("createdat").updatedAt("updatedat").build()`
   - Defaults: pk=`id`, no timestamps. When createdAt/updatedAt configured and value is null on insert, column omitted (DB DEFAULT applies)
3. **DaoRegistry** — obtain implementation: `DaoRegistry.forClass(UserDao.class, dataSource)` or with `objectMapper` as 3rd arg for JSON columns
4. **@GenerateMeta** on entity classes — optional compile-time bean introspection (GraalVM-friendly), discovered via ServiceLoader

## Two modes

- **Compile-time**: annotation processor generates impl, registered via ServiceLoader
- **Reflection fallback**: runtime proxy via `java.lang.reflect.Proxy` if no generated class found

## Column matching

Case-insensitive: `lower(propertyName) = lower(columnName)`. No camelCase-to-snake_case conversion. Use same naming in Java and SQL.

## Convention methods (no @Query)

Prefix determines operation, entity resolved by matching return/param type against EntityConfig fields in the interface:

- `load*` — `SELECT * WHERE pk=:id`, returns T, throws NoRowsException if not found
- `insert*` — INSERT (always, even with PK set)
- `update*` — `UPDATE SET ... WHERE pk=:id`
- `save*` — PK empty (null/0) → INSERT, otherwise → UPDATE
- `delete*` — `DELETE WHERE pk=:id` (accepts entity or raw PK value)

One interface can have multiple EntityConfig fields for different tables. Method name just needs to **start with** the prefix.

## @Query methods

Custom SQL with `:name` named parameters mapped from method param names.

```java
@Query("SELECT * FROM users WHERE email=:email")
Optional<User> findByEmail(String email);
```

### Return types

- `T` — exactly 1 row (throws NoRowsException if 0, TooManyRowsException if >1)
- `Optional<T>` — 0 or 1 row (throws TooManyRowsException if >1)
- `List<T>` — all rows
- `void` — execute INSERT/UPDATE/DELETE
- `int` — execute, returns affected row count
- Scalars (`String`, `Long`, `BigDecimal`, etc.) and `Optional<scalar>` for single-column selects

### Optional WHERE blocks

Wrap in `/** ... **/`. Block removed when param is null:

```java
@Query("SELECT * FROM users WHERE 1=1 /** AND id=:id **/ /** AND active=:active **/")
List<User> search(String id, Boolean active);
// search(null, true) → SELECT * FROM users WHERE 1=1 AND active=?
```

### Spread (IN clause)

`Spread<T>` param expands into positional params for IN:

```java
@Query("SELECT * FROM users WHERE id IN (:ids)")
List<User> findByIds(Spread<Integer> ids);
// Spread.of(10, 20, 30) → WHERE id IN (?,?,?)
```

Accepts varargs, array, or List. Throws if empty (unless inside optional `/** **/` block with null).

### Bean parameter expansion

Single bean/record param — its properties auto-expand as named params:

```java
record UserFilter(String name, String email) {}

@Query("SELECT * FROM users WHERE 1=1 /** AND name=:name **/ /** AND email=:email **/")
List<User> search(UserFilter filter);
// filter.name() → :name, filter.email() → :email
```

Applies only when: exactly one param, param is bean/record (not scalar, Spread, or ICondition).

## Dynamic WHERE (ICondition)

Use `ICondition` param with `:where` placeholder for complex dynamic conditions:

```java
@Query("SELECT * FROM users WHERE :where")
List<User> search(ICondition where);
```

Build conditions with `Condition.*` methods:

- `Condition.eq(col, value)` → `col = ?`
- `Condition.ne(col, value)` → `col != ?`
- `Condition.lt(col, value)` → `col < ?`
- `Condition.le(col, value)` → `col <= ?`
- `Condition.gt(col, value)` → `col > ?`
- `Condition.ge(col, value)` → `col >= ?`
- `Condition.like(col, value)` → `col LIKE ?`
- `Condition.in(col, values)` → `col IN (?,?,?)`
- `Condition.isNull(col)` → `col IS NULL`
- `Condition.isNotNull(col)` → `col IS NOT NULL`
- `Condition.between(col, from, to)` → `col BETWEEN ? AND ?`
- `Condition.raw(sql, params)` → raw SQL fragment
- `Condition.and(conds...)` → `(a AND b AND ...)`
- `Condition.or(conds...)` → `(a OR b OR ...)`
- `Condition.not(cond)` → `NOT (a)`

**Null values are silently skipped** — every value-based condition is automatically optional:

```java
Condition.and(Condition.eq("active", true), Condition.like("name", null))
// result: active = ?   (like skipped because value is null)
```

## JSON columns

Auto-serialized based on Java type — no annotations needed:
- JSON: custom beans/records, `List<T>`, `Set<T>`, `Map<K,V>`
- Not JSON: `String`, `int`, `long`, `boolean`, `BigDecimal`, `Instant`, `LocalDate`, etc.

Requires ObjectMapper passed to DaoRegistry. DaoException thrown if JSON column encountered without it.

## Exceptions

- `DaoException` — base class for all DAO exceptions (package `eu.aston.dao`)
- `NoRowsException` — 0 rows when T return type expected
- `TooManyRowsException` — >1 row when T or Optional<T> expected

## Testing

DAO vrstva sa testuje výhradne integračnými testami s reálnou databázou — žiadne mocky. Každý DAO musí mať 100% pokrytie integračnými testami, ktoré overujú skutočné SQL dotazy proti DB.

## Requirements

Java 17+, JDBC driver, Jackson ObjectMapper (optional, for JSON).
