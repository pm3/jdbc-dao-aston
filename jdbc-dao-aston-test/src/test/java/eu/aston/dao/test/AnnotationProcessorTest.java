package eu.aston.dao.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.aston.beanmeta.BeanMeta;
import eu.aston.beanmeta.BeanMetaRegistry;
import eu.aston.dao.DaoRegistry;
import eu.aston.dao.EntityConfig;
import eu.aston.dao.NoRowsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that annotation processors generated correct code at compile time. Verifies both BeanMetaProcessor
 * (@GenerateMeta) and DaoApiProcessor (@DaoApi).
 */
class AnnotationProcessorTest {

    private DataSource dataSource;
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        var ds = new org.h2.jdbcx.JdbcDataSource();
        ds.setURL("jdbc:h2:mem:aptest;DB_CLOSE_DELAY=-1");
        dataSource = ds;

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS users");
            st.execute("CREATE TABLE users (" + "id VARCHAR(50) PRIMARY KEY, " + "name VARCHAR(100), "
                    + "email VARCHAR(100), " + "active BOOLEAN DEFAULT true, "
                    + "createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP" + ")");
            st.execute("DROP TABLE IF EXISTS products");
            st.execute("CREATE TABLE products (" + "id VARCHAR(50) PRIMARY KEY, " + "name VARCHAR(100), "
                    + "price DOUBLE, " + "attrs VARCHAR(4000)" + ")");
        }
    }

    // ==================== @GenerateMeta — compile-time BeanMeta ====================

    @Test
    void generatedBeanMeta_existsViaServiceLoader() {
        // The processor should have generated TestUserMeta and registered it via ServiceLoader
        assertTrue(BeanMetaRegistry.hasCompiledMeta(TestUser.class),
                "TestUserMeta should be generated and registered via ServiceLoader");
        assertTrue(BeanMetaRegistry.hasCompiledMeta(TestProduct.class),
                "TestProductMeta should be generated and registered via ServiceLoader");
    }

    @Test
    void generatedBeanMeta_classExists() throws Exception {
        // Verify the generated class exists
        Class<?> metaClass = Class.forName("eu.aston.dao.test.TestUserMeta");
        assertNotNull(metaClass);
        assertTrue(BeanMeta.class.isAssignableFrom(metaClass));
    }

    @Test
    void generatedBeanMeta_type() {
        BeanMeta<TestUser> meta = BeanMetaRegistry.forClass(TestUser.class);
        assertEquals(TestUser.class, meta.type());
        // Should be the generated class, not reflective
        assertFalse(meta.getClass().getName().contains("Reflective"),
                "Should use generated meta, not reflective fallback");
    }

    @Test
    void generatedBeanMeta_names() {
        BeanMeta<TestUser> meta = BeanMetaRegistry.forClass(TestUser.class);
        assertEquals(List.of("id", "name", "email", "active", "createdat"), meta.names());
    }

    @Test
    void generatedBeanMeta_types() {
        BeanMeta<TestUser> meta = BeanMetaRegistry.forClass(TestUser.class);
        assertEquals(List.of(String.class, String.class, String.class, boolean.class, Instant.class), meta.types());
    }

    @Test
    void generatedBeanMeta_get() {
        BeanMeta<TestUser> meta = BeanMetaRegistry.forClass(TestUser.class);
        TestUser user = new TestUser("1", "John", "john@test.com", true, Instant.now());

        assertEquals("1", meta.get(user, "id"));
        assertEquals("John", meta.get(user, "name"));
        assertEquals("john@test.com", meta.get(user, "email"));
        assertEquals(true, meta.get(user, "active"));
        assertNotNull(meta.get(user, "createdat"));
    }

    @Test
    void generatedBeanMeta_create() {
        BeanMeta<TestUser> meta = BeanMetaRegistry.forClass(TestUser.class);
        Instant now = Instant.now();
        TestUser user = meta.create("1", "John", "john@test.com", true, now);

        assertEquals("1", user.id());
        assertEquals("John", user.name());
        assertEquals("john@test.com", user.email());
        assertTrue(user.active());
        assertEquals(now, user.createdat());
    }

    @Test
    void generatedBeanMeta_product_genericTypes() {
        BeanMeta<TestProduct> meta = BeanMetaRegistry.forClass(TestProduct.class);
        assertEquals(List.of("id", "name", "price", "attrs"), meta.names());
        // attrs is Map<String,String> — generic type should be ParameterizedType
        var genericTypes = meta.genericTypes();
        assertEquals(4, genericTypes.size());
        assertTrue(genericTypes.get(3) instanceof java.lang.reflect.ParameterizedType,
                "attrs should have ParameterizedType for Map<String,String>");
    }

    // ==================== @GenerateMeta vs Reflection — same results ====================

    /**
     * Non-annotated record for reflection comparison.
     */
    public record PlainUser(String id, String name, String email, boolean active, Instant createdat) {
    }

    @Test
    void generatedVsReflection_sameNames() {
        BeanMeta<TestUser> generated = BeanMetaRegistry.forClass(TestUser.class);
        BeanMeta<PlainUser> reflective = BeanMetaRegistry.forClass(PlainUser.class);

        assertFalse(BeanMetaRegistry.hasCompiledMeta(PlainUser.class), "PlainUser should use reflection");
        assertTrue(BeanMetaRegistry.hasCompiledMeta(TestUser.class), "TestUser should use generated");

        assertEquals(generated.names(), reflective.names());
        assertEquals(generated.types(), reflective.types());
    }

    @Test
    void generatedVsReflection_sameGetAndCreate() {
        BeanMeta<TestUser> generated = BeanMetaRegistry.forClass(TestUser.class);
        BeanMeta<PlainUser> reflective = BeanMetaRegistry.forClass(PlainUser.class);

        Instant now = Instant.now();
        TestUser genUser = generated.create("1", "John", "john@test.com", true, now);
        PlainUser refUser = reflective.create("1", "John", "john@test.com", true, now);

        assertEquals(generated.get(genUser, "id"), reflective.get(refUser, "id"));
        assertEquals(generated.get(genUser, "name"), reflective.get(refUser, "name"));
        assertEquals(generated.get(genUser, "active"), reflective.get(refUser, "active"));
    }

    // ==================== @DaoApi — compile-time DAO impl ====================

    @Test
    void generatedDaoImpl_classExists() throws Exception {
        Class<?> implClass = Class.forName("eu.aston.dao.test.TestUserDao$Impl");
        assertNotNull(implClass);
        assertTrue(TestUserDao.class.isAssignableFrom(implClass));
    }

    @Test
    void generatedDaoImpl_registeredViaServiceLoader() {
        // ServiceLoader should find the generated DaoProvider
        var providers = ServiceLoader.load(eu.aston.dao.DaoProvider.class);
        boolean found = false;
        for (var p : providers) {
            if (p.daoInterface() == TestUserDao.class) {
                found = true;
                break;
            }
        }
        assertTrue(found, "TestUserDao$Impl should be registered as DaoProvider via ServiceLoader");
    }

    @Test
    void generatedDao_crud() {
        TestUserDao dao = DaoRegistry.forClass(TestUserDao.class, dataSource);

        // Verify it's the generated impl, not proxy
        assertFalse(java.lang.reflect.Proxy.isProxyClass(dao.getClass()),
                "Should use generated impl, not reflection proxy");

        dao.insertUser(new TestUser("1", "John", "john@test.com", true, null));

        TestUser loaded = dao.loadById("1");
        assertEquals("1", loaded.id());
        assertEquals("John", loaded.name());
        assertEquals("john@test.com", loaded.email());
    }

    @Test
    void generatedDao_update() {
        TestUserDao dao = DaoRegistry.forClass(TestUserDao.class, dataSource);
        dao.insertUser(new TestUser("1", "John", "john@test.com", true, null));
        dao.updateUser(new TestUser("1", "Jane", "jane@test.com", false, null));

        assertEquals("Jane", dao.loadById("1").name());
    }

    @Test
    void generatedDao_delete() {
        TestUserDao dao = DaoRegistry.forClass(TestUserDao.class, dataSource);
        dao.insertUser(new TestUser("1", "John", "john@test.com", true, null));
        dao.deleteById("1");

        assertThrows(NoRowsException.class, () -> dao.loadById("1"));
    }

    @Test
    void generatedDao_queryOptional() {
        TestUserDao dao = DaoRegistry.forClass(TestUserDao.class, dataSource);
        dao.insertUser(new TestUser("1", "John", "john@test.com", true, null));

        Optional<TestUser> found = dao.findByEmail("john@test.com");
        assertTrue(found.isPresent());
        assertEquals("John", found.get().name());

        assertTrue(dao.findByEmail("nobody@test.com").isEmpty());
    }

    @Test
    void generatedDao_queryList() {
        TestUserDao dao = DaoRegistry.forClass(TestUserDao.class, dataSource);
        dao.insertUser(new TestUser("1", "John", "john@test.com", true, null));
        dao.insertUser(new TestUser("2", "Jane", "jane@test.com", true, null));

        assertEquals(2, dao.findAll().size());
    }

    @Test
    void generatedDao_queryScalar() {
        TestUserDao dao = DaoRegistry.forClass(TestUserDao.class, dataSource);
        dao.insertUser(new TestUser("1", "John", "john@test.com", true, null));
        dao.insertUser(new TestUser("2", "Jane", "jane@test.com", true, null));

        assertEquals(2, dao.countAll());
    }

    @Test
    void generatedDao_queryVoid() {
        TestUserDao dao = DaoRegistry.forClass(TestUserDao.class, dataSource);
        dao.insertUser(new TestUser("1", "John", "john@test.com", true, null));
        dao.deleteAll();

        assertEquals(0, dao.countAll());
    }

    // ==================== @DaoApi with JSON columns ====================

    @Test
    void generatedDao_jsonColumn() {
        TestProductDao dao = DaoRegistry.forClass(TestProductDao.class, dataSource, objectMapper);
        dao.insertProduct(new TestProduct("1", "Widget", 19.99, Map.of("color", "red")));

        TestProduct loaded = dao.loadById("1");
        assertEquals("Widget", loaded.name());
        assertEquals("red", loaded.attrs().get("color"));
    }

    // ==================== Generated vs Proxy — same behavior ====================

    /**
     * Non-annotated DAO for proxy comparison.
     */
    public interface PlainUserDao {
        EntityConfig<TestUser> USER = EntityConfig.of(TestUser.class, "users").updatedAt("").build();

        TestUser loadById(String id);

        void insertUser(TestUser user);

        @eu.aston.dao.Query("SELECT * FROM users")
        List<TestUser> findAll();

        @eu.aston.dao.Query("SELECT count(*) FROM users")
        long countAll();
    }

    @Test
    void generatedVsProxy_sameBehavior() {
        // Generated (TestUserDao has @DaoApi, processor generated impl)
        TestUserDao generated = DaoRegistry.forClass(TestUserDao.class, dataSource);
        // Proxy (PlainUserDao has no @DaoApi, falls back to reflection proxy)
        PlainUserDao proxy = DaoRegistry.forClass(PlainUserDao.class, dataSource);

        assertFalse(java.lang.reflect.Proxy.isProxyClass(generated.getClass()),
                "TestUserDao should use generated impl");
        assertTrue(java.lang.reflect.Proxy.isProxyClass(proxy.getClass()), "PlainUserDao should use reflection proxy");

        // Both should produce same results
        generated.insertUser(new TestUser("1", "John", "john@test.com", true, null));

        TestUser fromGenerated = generated.loadById("1");
        TestUser fromProxy = proxy.loadById("1");

        assertEquals(fromGenerated.id(), fromProxy.id());
        assertEquals(fromGenerated.name(), fromProxy.name());
        assertEquals(fromGenerated.email(), fromProxy.email());

        assertEquals(generated.countAll(), proxy.countAll());
        assertEquals(generated.findAll().size(), proxy.findAll().size());
    }
}
