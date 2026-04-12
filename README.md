# jdbc-dao-aston

Lightweight JDBC DAO library for Java. No ORM, no magic — just clean SQL with named parameters, optional WHERE blocks, and entity helpers. DAO interfaces are defined declaratively — implementation is generated at compile time via annotation processor (with reflection fallback).

## Installation

**Maven:**
```xml
<dependency>
    <groupId>eu.aston</groupId>
    <artifactId>jdbc-dao-aston</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Annotation processor (compile-time only) -->
<dependency>
    <groupId>eu.aston</groupId>
    <artifactId>jdbc-dao-aston-processor</artifactId>
    <version>1.0.0</version>
    <scope>provided</scope>
</dependency>
```

**Gradle:**
```groovy
implementation 'eu.aston:jdbc-dao-aston:1.0.0'
annotationProcessor 'eu.aston:jdbc-dao-aston-processor:1.0.0'
```

## Quick Start

```java
@DaoApi
public interface UserDao {

    EntityConfig<User> USER = EntityConfig.of(User.class, "users").build();

    User loadById(String id);

    void insertUser(User user);

    @Query("SELECT * FROM users WHERE email=:email")
    Optional<User> findByEmail(String email);

    @Query("SELECT * FROM users WHERE active=:active")
    List<User> findActive(boolean active);
}
```

```java
UserDao userDao = DaoRegistry.forClass(UserDao.class, dataSource);

User user = userDao.loadById("123");
```

---

## Two modes, one API

Compile-time generation with reflection fallback:

| Mode | How | When |
|------|-----|------|
| **Compile-time** | Annotation processor generates DAO implementation class + bean meta classes, registered via `ServiceLoader` | `@DaoApi` on interface and/or `@GenerateMeta` on record/bean + processor on classpath |
| **Reflection fallback** | Runtime proxy via `java.lang.reflect.Proxy`, bean introspection via reflection | No generated class found for the given interface/bean |

The annotation processor generates two types of classes:
- **DAO implementations** (`@DaoApi`) — generated `*$Impl` classes with pre-resolved SQL and method dispatch
- **Bean metadata** (`@GenerateMeta`) — generated `*Meta` classes with optimized property access (no reflection)

Both are discovered automatically via `ServiceLoader`. If no generated class is found, the library falls back to reflection automatically.

---

## Entity Definition

Entities are defined as `static EntityConfig` fields inside the interface. One interface can work with **multiple entities/tables**.

```java
@DaoApi
public interface UserDao {

    EntityConfig<User> USER = EntityConfig.of(User.class, "users").build();
    EntityConfig<AuditLog> AUDIT = EntityConfig.of(AuditLog.class, "audit_log").build();

    // ...
}
```

### EntityConfig options

```java
// minimal — table name only, defaults for pk/timestamps
EntityConfig<User> USER = EntityConfig.of(User.class, "users").build();

// full control
EntityConfig<User> USER = EntityConfig.of(User.class, "users")
    .pk("userid")
    .createdAt("createdat")
    .updatedAt("updatedat")
    .build();
```

| Config | Default | Override |
|---|---|---|
| Primary key | `id` | `.pk("customid")` |
| Created timestamp | *(none)* | `.createdAt("createdat")` |
| Updated timestamp | *(none)* | `.updatedAt("updatedat")` |

All columns are always sent in INSERT/UPDATE statements — including timestamps. If you want DB defaults, set the values explicitly in Java before inserting.

### Column name matching

Bean property names are matched to SQL column names **case-insensitively** — `lower(propertyName) = lower(columnName)`. No camelCase-to-snake_case conversion. Use the same naming in both Java and SQL:

```java
// Java record
public record User(String id, String name, String extid, Instant createdat) {}

// SQL table
// CREATE TABLE users (id text, name text, extid text, createdat timestamptz)
```

### Compile-time bean introspection (@GenerateMeta)

By default, entity classes are introspected at runtime via reflection. For better performance or GraalVM native image support, annotate your data classes with `@GenerateMeta` — the annotation processor will generate optimized `*Meta` classes at compile time.

```java
import eu.aston.beanmeta.GenerateMeta;

@GenerateMeta
public record User(String id, String name, String extid, Instant createdat) {}
```

This is **optional** — without the annotation, everything works the same via reflection fallback. The generated meta class is discovered automatically via `ServiceLoader`.

---

## Entity Methods (convention-based)

Methods without `@Query` are recognized by **name prefix** and operate on the entity type inferred from the return type or parameter type. The method name doesn't have to match the prefix exactly — it just has to **start with** it.

| Prefix | Operation | Example signatures |
|---|---|---|
| `load` | SELECT * WHERE pk=:id — always returns `T`, throws `NoRowsException` if not found. For `Optional`, use `@Query` | `User loadById(String id)`, `User loadUser(String id)` |
| `insert` | INSERT INTO ... all columns including PK | `void insert(User u)`, `void insertUser(User u)` |
| `update` | UPDATE SET ... all non-PK columns, WHERE pk=:id | `void update(User u)`, `void updateUser(User u)` |
| `save` | PK is empty (`null` or `0`) → INSERT without PK (auto-increment), otherwise → UPDATE | `void save(User u)`, `void saveUser(User u)` |
| `delete` | DELETE WHERE pk=:id (accepts entity or PK value) | `void delete(User u)`, `void deleteById(String id)` |

```java
@DaoApi
public interface UserDao {

    EntityConfig<User> USER = EntityConfig.of(User.class, "users").build();
    EntityConfig<AuditLog> AUDIT = EntityConfig.of(AuditLog.class, "audit_log").build();

    User loadById(String id);              // loads from USER (matched by return type)

    void insertUser(User user);            // inserts into USER
    void insertAudit(AuditLog log);        // inserts into AUDIT (matched by param type)

    void saveUser(User user);              // upsert into USER

    void updateUser(User user);            // update USER

    void deleteUser(User user);            // delete from USER
    void deleteById(String id);            // delete from USER by PK
}
```

The entity config is resolved by matching the bean class (`User`, `AuditLog`) against the `EntityConfig` static fields defined in the interface.

---

## @Query Methods

Custom SQL with named parameters. Method parameter names map directly to `:name` placeholders in SQL. Parameter names are resolved at compile time by the annotation processor (or at runtime via `-parameters` compiler flag in reflection fallback mode).

```java
@DaoApi
public interface UserDao {

    EntityConfig<User> USER = EntityConfig.of(User.class, "users").build();

    @Query("SELECT * FROM users WHERE id=:id")
    User load1(String id);

    @Query("SELECT * FROM users WHERE id=:id")
    Optional<User> loadO1(String id);

    @Query("SELECT id FROM users WHERE id=:id")
    Optional<String> loadO2(String id);

    @Query("SELECT * FROM users")
    List<User> loadAll();

    @Query("INSERT INTO users (id, name, props) VALUES (:id, :name, :props)")
    void insert2(String id, String name, Map<String, String> props);

    @Query("UPDATE users SET props=:props WHERE id=:id")
    void update2(String id, Map<String, String> props);

    @Query("DELETE FROM users")
    void deleteAll();
}
```

### Return type semantics

The return type determines how the result is processed:

| Return type | Behavior |
|---|---|
| `T` | Expects exactly 1 row. Throws `NoRowsException` if 0, `TooManyRowsException` if >1 |
| `Optional<T>` | Returns `Optional.empty()` if 0 rows. Throws `TooManyRowsException` if >1 |
| `List<T>` | Returns all rows |
| `void` | Execute (INSERT/UPDATE/DELETE), no return value |
| `int` | Execute, returns affected row count |

### Scalar return types

When selecting a single column, the return type can be a scalar (`String`, `Integer`, `Long`, `BigDecimal`, etc.) or `Optional<scalar>`:

```java
@Query("SELECT count(*) FROM users WHERE active=:active")
long countActive(boolean active);

@Query("SELECT email FROM users WHERE id=:id")
Optional<String> findEmailById(String id);
```

---

## Bean Parameter Expansion

When a `@Query` method has a **single parameter** that is a bean or record (not a scalar type), its properties are automatically expanded as named parameters. This avoids manually listing each field:

```java
public record UserFilter(String name, String email) {}

@DaoApi
public interface UserDao {

    EntityConfig<User> USER = EntityConfig.of(User.class, "users").build();

    @Query("SELECT * FROM users WHERE name=:name AND email=:email")
    List<User> findByFilter(UserFilter filter);
}
```

```java
// filter.name() → :name, filter.email() → :email
List<User> users = userDao.findByFilter(new UserFilter("John", "john@test.com"));
```

This works with optional blocks too:

```java
@Query("SELECT * FROM users WHERE 1=1 /** AND name=:name **/ /** AND email=:email **/")
List<User> search(UserFilter filter);
```

Bean expansion applies only when:
- There is exactly **one** parameter
- The parameter type is a **bean or record** (not a scalar, `Spread`, or `ICondition`)

For multiple parameters or scalar types, standard named parameter mapping is used.

---

## Optional WHERE Blocks

Wrap optional conditions in `/** ... **/` comments. The block is removed if the parameter is `null`.

```java
@Query("""
    SELECT * FROM users WHERE 1=1
    /** AND id=:id **/
    /** AND extid=:extid **/
    /** AND active=:active **/
    """)
List<User> search(String id, String extid, Boolean active);
```

```java
// call with id=null, extid="abc", active=true
List<User> users = userDao.search(null, "abc", true);

// generated SQL:
// SELECT * FROM users WHERE 1=1 AND extid=? AND active=?
```

---

## Array Parameters (IN clause)

Use `Spread.of()` to expand values into multiple positional parameters for `IN` clauses.

```java
@Query("SELECT * FROM users WHERE id IN (:ids) AND status=:status")
List<User> findByIds(Spread<Integer> ids, String status);
```

`Spread.of()` accepts varargs, array, or `List`:

```java
// varargs
userDao.findByIds(Spread.of(10, 20, 30), "active");

// array
Integer[] ids = {10, 20, 30};
userDao.findByIds(Spread.of(ids), "active");

// List
List<Integer> idList = List.of(10, 20, 30);
userDao.findByIds(Spread.of(idList), "active");

// generated SQL (all three):
// SELECT * FROM users WHERE id IN (?,?,?) AND status=?
```

`Spread.of()` throws if empty — unless used inside an optional `/** **/` block, where a `null` param removes the block as usual.

---

## Dynamic WHERE (ICondition)

For complex dynamic conditions, use `ICondition` as a parameter with `:where` placeholder:

```java
@Query("SELECT * FROM users WHERE :where")
List<User> search(ICondition where);
```

```java
ICondition cond = Condition.and(
    Condition.eq("active", true),
    Condition.like("name", "%ano%")
);
List<User> users = userDao.search(cond);
```

### Condition methods

| Method | SQL | Example |
|---|---|---|
| `Condition.eq(col, value)` | `col = ?` | `Condition.eq("active", true)` |
| `Condition.ne(col, value)` | `col != ?` | `Condition.ne("status", "deleted")` |
| `Condition.lt(col, value)` | `col < ?` | `Condition.lt("age", 18)` |
| `Condition.le(col, value)` | `col <= ?` | `Condition.le("age", 65)` |
| `Condition.gt(col, value)` | `col > ?` | `Condition.gt("score", 100)` |
| `Condition.ge(col, value)` | `col >= ?` | `Condition.ge("score", 0)` |
| `Condition.like(col, value)` | `col LIKE ?` | `Condition.like("name", "%ano%")` |
| `Condition.in(col, values)` | `col IN (?,?,?)` | `Condition.in("id", List.of(1,2,3))` |
| `Condition.isNull(col)` | `col IS NULL` | `Condition.isNull("deletedat")` |
| `Condition.isNotNull(col)` | `col IS NOT NULL` | `Condition.isNotNull("email")` |
| `Condition.between(col, from, to)` | `col BETWEEN ? AND ?` | `Condition.between("age", 18, 65)` |
| `Condition.raw(sql, params)` | raw SQL fragment | `Condition.raw("lower(name)=?", "john")` |
| `Condition.and(conds...)` | `(a AND b AND ...)` | `Condition.and(c1, c2, c3)` |
| `Condition.or(conds...)` | `(a OR b OR ...)` | `Condition.or(c1, c2)` |
| `Condition.not(cond)` | `NOT (a)` | `Condition.not(c1)` |

If a value parameter is `null`, the condition is silently skipped. Every value-based condition is automatically optional:

```java
ICondition cond = Condition.and(
    Condition.eq("active", true),
    Condition.like("name", null),   // skipped — value is null
    Condition.gt("age", null)       // skipped — value is null
);
// result: active = ?
```

---

## JSON Columns

JSON serialization is automatic based on Java type — no annotations needed. Works for both entity methods and `@Query` parameters.

| Java type | JSON serialization |
|---|---|
| Custom bean/record | yes |
| `List<T>`, `Set<T>` | yes |
| `Map<K,V>` | yes |
| `String`, `int`, `long`, `boolean`, `BigDecimal`, `Instant`, `LocalDate`, ... | no |

---

## Configuration

```java
// Default — DataSource only, no JSON support
UserDao userDao = DaoRegistry.forClass(UserDao.class, dataSource);

// With ObjectMapper — required for JSON column serialization
UserDao userDao = DaoRegistry.forClass(UserDao.class, dataSource, objectMapper);
```

If a JSON column is encountered (read or write) and no `ObjectMapper` was provided, a `DaoException` is thrown.

---

## Error Handling

```java
import eu.aston.dao.NoRowsException;
import eu.aston.dao.TooManyRowsException;
import eu.aston.dao.DaoException;

try {
    User user = userDao.loadById("999");
} catch (NoRowsException e) {
    // no row found
}
```

| Exception | Thrown by | When |
|---|---|---|
| `NoRowsException` | `T` return type methods, `loadById` | 0 rows returned |
| `TooManyRowsException` | `T` and `Optional<T>` return type methods | more than 1 row returned |
| `DaoException` | base class | parent of all DAO exceptions |

---

## Testing

DAO interfaces cleanly separate all database access from application logic. In the consuming project, each DAO can be tested independently via integration tests against a real database — no mocks, 100% control over every SQL query, parameter binding, and result mapping.

---

## Requirements

- Java 17+
- JDBC driver for your database (PostgreSQL, MySQL, etc.)
- Jackson `ObjectMapper` (optional — pass to `DaoRegistry.forClass()` if using JSON columns)

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
