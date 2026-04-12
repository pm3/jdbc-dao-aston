package eu.aston.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests against PostgreSQL running in a Docker container.
 * Container is started/stopped automatically via @BeforeAll/@AfterAll.
 */
class PostgresIntegrationTest {

    private static final String CONTAINER_NAME = "jdbc-dao-test-pg";
    private static final String PG_USER = "test";
    private static final String PG_PASS = "test";
    private static final String PG_DB = "test";

    private static DataSource dataSource;
    private static int pgPort;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // --- Entities ---

    public record User(String id, String name, String email, boolean active, Instant createdat) {}

    public record Product(String id, String name, double price, Map<String, String> attrs) {}

    public record AuditLog(long id, String action, String details) {}

    // --- DAOs ---

    @DaoApi
    public interface UserDao {
        EntityConfig<User> USER = EntityConfig.of(User.class, "users")
                .updatedAt("").build();

        User loadById(String id);
        void insertUser(User user);
        void updateUser(User user);
        void saveUser(User user);
        void deleteUser(User user);
        void deleteById(String id);

        @Query("SELECT * FROM users WHERE email=:email")
        Optional<User> findByEmail(String email);

        @Query("SELECT * FROM users WHERE 1=1 /** AND name=:name **/ /** AND active=:active **/")
        List<User> search(String name, Boolean active);

        @Query("SELECT * FROM users")
        List<User> findAll();

        @Query("SELECT * FROM users WHERE id IN (:ids)")
        List<User> findByIds(Spread<String> ids);

        @Query("SELECT * FROM users WHERE :where ORDER BY id")
        List<User> findWhere(ICondition where);

        @Query("SELECT count(*) FROM users")
        long countAll();

        @Query("SELECT count(*) FROM users WHERE active=:active")
        long countActive(boolean active);

        @Query("SELECT name FROM users WHERE id=:id")
        Optional<String> findNameById(String id);

        @Query("DELETE FROM users")
        void deleteAll();

        @Query("UPDATE users SET active=:active WHERE id=:id")
        int setActive(String id, boolean active);
    }

    @DaoApi
    public interface ProductDao {
        EntityConfig<Product> PRODUCT = EntityConfig.of(Product.class, "products")
                .createdAt("").updatedAt("").build();

        void insertProduct(Product product);
        Product loadById(String id);

        @Query("SELECT * FROM products WHERE :where")
        List<Product> search(ICondition where);
    }

    @DaoApi
    public interface AuditDao {
        EntityConfig<AuditLog> AUDIT = EntityConfig.of(AuditLog.class, "audit_log")
                .createdAt("").updatedAt("").build();

        void insertAudit(AuditLog log);
        void saveAudit(AuditLog log);

        @Query("SELECT * FROM audit_log ORDER BY id")
        List<AuditLog> findAll();
    }

    // --- Docker lifecycle ---

    @BeforeAll
    static void startPostgres() throws Exception {
        // kill any leftover container
        exec("docker", "rm", "-f", CONTAINER_NAME);

        // start fresh container
        exec("docker", "run", "-d", "--name", CONTAINER_NAME,
                "-e", "POSTGRES_USER=" + PG_USER,
                "-e", "POSTGRES_PASSWORD=" + PG_PASS,
                "-e", "POSTGRES_DB=" + PG_DB,
                "-p", "0:5432",
                "postgres:17");

        // resolve mapped port
        pgPort = Integer.parseInt(
                execOutput("docker", "port", CONTAINER_NAME, "5432")
                        .replaceAll(".*:", "").trim());

        // wait for postgres to be ready
        for (int i = 0; i < 30; i++) {
            try {
                int exitCode = exec("docker", "exec", CONTAINER_NAME, "pg_isready", "-U", PG_USER);
                if (exitCode == 0) break;
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }

        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{"localhost"});
        ds.setPortNumbers(new int[]{pgPort});
        ds.setDatabaseName(PG_DB);
        ds.setUser(PG_USER);
        ds.setPassword(PG_PASS);
        dataSource = ds;
    }

    @AfterAll
    static void stopPostgres() throws Exception {
        exec("docker", "rm", "-f", CONTAINER_NAME);
    }

    @BeforeEach
    void createTables() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS users");
            st.execute("CREATE TABLE users ("
                    + "id VARCHAR(50) PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "email VARCHAR(100), "
                    + "active BOOLEAN DEFAULT true, "
                    + "createdat TIMESTAMPTZ DEFAULT now()"
                    + ")");

            st.execute("DROP TABLE IF EXISTS products");
            st.execute("CREATE TABLE products ("
                    + "id VARCHAR(50) PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "price DOUBLE PRECISION, "
                    + "attrs JSONB"
                    + ")");

            st.execute("DROP TABLE IF EXISTS audit_log");
            st.execute("DROP SEQUENCE IF EXISTS audit_log_seq");
            st.execute("CREATE SEQUENCE audit_log_seq");
            st.execute("CREATE TABLE audit_log ("
                    + "id BIGINT PRIMARY KEY DEFAULT nextval('audit_log_seq'), "
                    + "action VARCHAR(100), "
                    + "details TEXT"
                    + ")");
        }
    }

    // --- Docker helpers ---

    private static int exec(String... cmd) throws Exception {
        return new ProcessBuilder(cmd).inheritIO().start().waitFor();
    }

    private static String execOutput(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        try (var reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line = reader.readLine();
            p.waitFor();
            return line != null ? line : "";
        }
    }

    // ==================== CRUD tests ====================

    @Test
    void insertAndLoad() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        dao.insertUser(new User("1", "John", "john@test.com", true, now));

        User loaded = dao.loadById("1");
        assertEquals("1", loaded.id());
        assertEquals("John", loaded.name());
        assertEquals("john@test.com", loaded.email());
        assertTrue(loaded.active());
        assertNotNull(loaded.createdat());
    }

    @Test
    void insertWithNullTimestamp_usesDbDefault() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));

        User loaded = dao.loadById("1");
        assertNotNull(loaded.createdat(), "DB should set createdat default");
    }

    @Test
    void loadNotFound_throws() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        assertThrows(NoRowsException.class, () -> dao.loadById("nonexistent"));
    }

    @Test
    void update() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.updateUser(new User("1", "Jane", "jane@test.com", false, null));

        User loaded = dao.loadById("1");
        assertEquals("Jane", loaded.name());
        assertEquals("jane@test.com", loaded.email());
        assertFalse(loaded.active());
    }

    @Test
    void save_existingPk_updates() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.saveUser(new User("1", "Updated", "updated@test.com", true, null));

        assertEquals("Updated", dao.loadById("1").name());
    }

    @Test
    void deleteByEntity() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.deleteUser(new User("1", null, null, false, null));

        assertThrows(NoRowsException.class, () -> dao.loadById("1"));
    }

    @Test
    void deleteByPk() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.deleteById("1");

        assertThrows(NoRowsException.class, () -> dao.loadById("1"));
    }

    // ==================== @Query tests ====================

    @Test
    void query_optional_found() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));

        Optional<User> found = dao.findByEmail("john@test.com");
        assertTrue(found.isPresent());
        assertEquals("John", found.get().name());
    }

    @Test
    void query_optional_notFound() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        assertTrue(dao.findByEmail("nobody@test.com").isEmpty());
    }

    @Test
    void query_list() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        assertEquals(2, dao.findAll().size());
    }

    @Test
    void query_void_deleteAll() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", true, null));
        dao.deleteAll();

        assertEquals(0, dao.findAll().size());
    }

    @Test
    void query_int_returnAffectedRows() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));

        assertEquals(1, dao.setActive("1", false));
        assertEquals(0, dao.setActive("nonexistent", false));
    }

    @Test
    void query_scalarCount() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        assertEquals(2, dao.countAll());
        assertEquals(1, dao.countActive(true));
        assertEquals(1, dao.countActive(false));
    }

    @Test
    void query_scalarOptionalString() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));

        assertEquals("John", dao.findNameById("1").orElse(null));
        assertTrue(dao.findNameById("nonexistent").isEmpty());
    }

    // ==================== Optional WHERE blocks ====================

    @Test
    void optionalBlocks_allNull() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        assertEquals(2, dao.search(null, null).size());
    }

    @Test
    void optionalBlocks_nameOnly() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        List<User> result = dao.search("John", null);
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).id());
    }

    @Test
    void optionalBlocks_activeOnly() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        assertEquals(1, dao.search(null, true).size());
        assertEquals(1, dao.search(null, false).size());
    }

    @Test
    void optionalBlocks_bothPresent() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        assertEquals(1, dao.search("John", true).size());
        assertEquals(0, dao.search("John", false).size());
    }

    // ==================== Spread (IN clause) ====================

    @Test
    void spread_varargs() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", true, null));
        dao.insertUser(new User("3", "Bob", "bob@test.com", true, null));

        assertEquals(2, dao.findByIds(Spread.of("1", "3")).size());
    }

    @Test
    void spread_singleValue() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));

        assertEquals(1, dao.findByIds(Spread.of("1")).size());
    }

    @Test
    void spread_fromList() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", true, null));

        assertEquals(2, dao.findByIds(Spread.of(List.of("1", "2"))).size());
    }

    // ==================== ICondition (dynamic WHERE) ====================

    @Test
    void condition_simpleEq() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        List<User> result = dao.findWhere(Condition.eq("active", true));
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).id());
    }

    @Test
    void condition_andComposite() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", true, null));
        dao.insertUser(new User("3", "Bob", "bob@test.com", false, null));

        List<User> result = dao.findWhere(Condition.and(
                Condition.eq("active", true),
                Condition.like("name", "Jo%")
        ));
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).id());
    }

    @Test
    void condition_or() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", true, null));
        dao.insertUser(new User("3", "Bob", "bob@test.com", false, null));

        assertEquals(2, dao.findWhere(Condition.or(
                Condition.eq("name", "John"),
                Condition.eq("name", "Bob")
        )).size());
    }

    @Test
    void condition_in() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", true, null));
        dao.insertUser(new User("3", "Bob", "bob@test.com", false, null));

        assertEquals(2, dao.findWhere(Condition.in("id", List.of("1", "3"))).size());
    }

    @Test
    void condition_between() {
        ProductDao dao = DaoRegistry.forClass(ProductDao.class, dataSource, objectMapper);
        dao.insertProduct(new Product("1", "Cheap", 10.0, Map.of()));
        dao.insertProduct(new Product("2", "Mid", 50.0, Map.of()));
        dao.insertProduct(new Product("3", "Expensive", 100.0, Map.of()));

        List<Product> result = dao.search(Condition.between("price", 20.0, 80.0));
        assertEquals(1, result.size());
        assertEquals("2", result.get(0).id());
    }

    @Test
    void condition_isNull_isNotNull() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", null, true, null));

        assertEquals(1, dao.findWhere(Condition.isNotNull("email")).size());
        List<User> noEmail = dao.findWhere(Condition.isNull("email"));
        assertEquals(1, noEmail.size());
        assertEquals("2", noEmail.get(0).id());
    }

    @Test
    void condition_nullValuesSkipped() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        assertEquals(2, dao.findWhere(Condition.and(
                Condition.eq("name", null),
                Condition.like("email", null)
        )).size());
    }

    @Test
    void condition_not() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", false, null));

        List<User> result = dao.findWhere(Condition.not(Condition.eq("active", true)));
        assertEquals(1, result.size());
        assertEquals("2", result.get(0).id());
    }

    @Test
    void condition_raw() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com", true, null));
        dao.insertUser(new User("2", "Jane", "jane@test.com", true, null));

        assertEquals(1, dao.findWhere(Condition.raw("lower(name) = ?", "john")).size());
    }

    // ==================== JSON columns ====================

    @Test
    void jsonColumn_insertAndLoad() {
        ProductDao dao = DaoRegistry.forClass(ProductDao.class, dataSource, objectMapper);
        dao.insertProduct(new Product("1", "Widget", 19.99, Map.of("color", "red", "size", "L")));

        Product loaded = dao.loadById("1");
        assertEquals("Widget", loaded.name());
        assertEquals(19.99, loaded.price(), 0.001);
        assertEquals("red", loaded.attrs().get("color"));
        assertEquals("L", loaded.attrs().get("size"));
    }

    @Test
    void jsonColumn_nullValue() {
        ProductDao dao = DaoRegistry.forClass(ProductDao.class, dataSource, objectMapper);
        dao.insertProduct(new Product("1", "Widget", 19.99, null));

        assertNull(dao.loadById("1").attrs());
    }

    @Test
    void jsonColumn_withoutObjectMapper_throws() {
        ProductDao dao = DaoRegistry.forClass(ProductDao.class, dataSource);
        assertThrows(DaoException.class, () ->
                dao.insertProduct(new Product("1", "Widget", 19.99, Map.of("k", "v"))));
    }

    // ==================== Multi-entity DAO ====================

    @Test
    void multiEntityDao_insertDifferentTables() {
        UserDao userDao = DaoRegistry.forClass(UserDao.class, dataSource);
        ProductDao productDao = DaoRegistry.forClass(ProductDao.class, dataSource, objectMapper);

        userDao.insertUser(new User("u1", "John", "john@test.com", true, null));
        productDao.insertProduct(new Product("p1", "Widget", 9.99, Map.of()));

        assertEquals("John", userDao.loadById("u1").name());
        assertEquals("Widget", productDao.loadById("p1").name());
    }

    // ==================== Save with auto-increment PK ====================

    @Test
    void save_zeroPk_inserts() {
        AuditDao dao = DaoRegistry.forClass(AuditDao.class, dataSource);
        // save with zero PK triggers insert, DB sequence generates the PK
        dao.saveAudit(new AuditLog(0, "CREATE", "created something"));

        List<AuditLog> all = dao.findAll();
        assertEquals(1, all.size());
        assertTrue(all.get(0).id() > 0, "DB should generate PK via sequence");
        assertEquals("CREATE", all.get(0).action());
    }

    // ==================== Bulk operations ====================

    @Test
    void multipleInsertsAndSearches() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        for (int i = 0; i < 100; i++) {
            dao.insertUser(new User("u" + i, "User" + i, "user" + i + "@test.com", i % 2 == 0, null));
        }
        assertEquals(100, dao.countAll());
        assertEquals(50, dao.countActive(true));
        assertEquals(50, dao.countActive(false));
        assertEquals(50, dao.search(null, true).size());
    }
}
