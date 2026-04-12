package eu.aston.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DaoProxyTest {

    private DataSource dataSource;
    private ObjectMapper objectMapper;

    // --- Test entity ---

    public record User(String id, String name, String email) {}

    // --- Test DAO interface ---

    @DaoApi
    public interface UserDao {

        EntityConfig<User> USER = EntityConfig.of(User.class, "users")
                .createdAt("").updatedAt("").build();

        User loadById(String id);

        void insertUser(User user);

        void updateUser(User user);

        void saveUser(User user);

        void deleteUser(User user);

        void deleteById(String id);

        @Query("SELECT * FROM users WHERE email=:email")
        Optional<User> findByEmail(String email);

        @Query("SELECT * FROM users WHERE 1=1 /** AND name=:name **/ /** AND email=:email **/")
        List<User> search(String name, String email);

        @Query("SELECT * FROM users")
        List<User> findAll();

        @Query("DELETE FROM users")
        void deleteAll();

        @Query("SELECT count(*) FROM users")
        long countAll();

        @Query("SELECT * FROM users WHERE name=:name AND email=:email")
        List<User> findByFilter(UserFilter filter);
    }

    public record UserFilter(String name, String email) {}

    // --- JSON test ---

    public record Item(String id, String name, Map<String, String> props) {}

    @DaoApi
    public interface ItemDao {

        EntityConfig<Item> ITEM = EntityConfig.of(Item.class, "items")
                .createdAt("").updatedAt("").build();

        void insertItem(Item item);

        Item loadById(String id);
    }

    @BeforeEach
    void setUp() throws Exception {
        var ds = new org.h2.jdbcx.JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        this.dataSource = ds;
        this.objectMapper = new ObjectMapper();

        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS users");
            st.execute("CREATE TABLE users (id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))");
            st.execute("DROP TABLE IF EXISTS items");
            st.execute("CREATE TABLE items (id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), props VARCHAR(4000))");
        }
    }

    @Test
    void insertAndLoad() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        User loaded = dao.loadById("1");
        assertEquals("1", loaded.id());
        assertEquals("John", loaded.name());
        assertEquals("john@test.com", loaded.email());
    }

    @Test
    void loadNotFound() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        assertThrows(NoRowsException.class, () -> dao.loadById("999"));
    }

    @Test
    void updateEntity() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.updateUser(new User("1", "Jane", "jane@test.com"));
        User loaded = dao.loadById("1");
        assertEquals("Jane", loaded.name());
        assertEquals("jane@test.com", loaded.email());
    }

    @Test
    void saveInsertAndUpdate() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        // save with non-null PK acts as insert for string PK since it's not empty
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.saveUser(new User("1", "Jane", "jane@test.com"));
        assertEquals("Jane", dao.loadById("1").name());
    }

    @Test
    void deleteByEntity() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.deleteUser(new User("1", "John", "john@test.com"));
        assertThrows(NoRowsException.class, () -> dao.loadById("1"));
    }

    @Test
    void deleteByPk() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.deleteById("1");
        assertThrows(NoRowsException.class, () -> dao.loadById("1"));
    }

    @Test
    void queryFindByEmail() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        Optional<User> found = dao.findByEmail("john@test.com");
        assertTrue(found.isPresent());
        assertEquals("John", found.get().name());

        Optional<User> notFound = dao.findByEmail("nobody@test.com");
        assertTrue(notFound.isEmpty());
    }

    @Test
    void queryFindAll() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.insertUser(new User("2", "Jane", "jane@test.com"));
        List<User> all = dao.findAll();
        assertEquals(2, all.size());
    }

    @Test
    void queryOptionalBlocks() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.insertUser(new User("2", "Jane", "jane@test.com"));

        // both null -> all results
        assertEquals(2, dao.search(null, null).size());
        // name only
        assertEquals(1, dao.search("John", null).size());
        // email only
        assertEquals(1, dao.search(null, "jane@test.com").size());
        // both
        assertEquals(1, dao.search("John", "john@test.com").size());
        // no match
        assertEquals(0, dao.search("Nobody", null).size());
    }

    @Test
    void queryDeleteAll() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.insertUser(new User("2", "Jane", "jane@test.com"));
        dao.deleteAll();
        assertEquals(0, dao.findAll().size());
    }

    @Test
    void queryCount() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.insertUser(new User("2", "Jane", "jane@test.com"));
        assertEquals(2, dao.countAll());
    }

    @Test
    void jsonColumns() {
        ItemDao dao = DaoRegistry.forClass(ItemDao.class, dataSource, objectMapper);
        dao.insertItem(new Item("1", "Widget", Map.of("color", "red", "size", "L")));
        Item loaded = dao.loadById("1");
        assertEquals("1", loaded.id());
        assertEquals("Widget", loaded.name());
        assertEquals("red", loaded.props().get("color"));
        assertEquals("L", loaded.props().get("size"));
    }

    // --- Bean param expansion ---

    @Test
    void beanParamExpand() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));
        dao.insertUser(new User("2", "Jane", "jane@test.com"));

        List<User> results = dao.findByFilter(new UserFilter("John", "john@test.com"));
        assertEquals(1, results.size());
        assertEquals("1", results.get(0).id());
    }

    @Test
    void beanParamExpandNoMatch() {
        UserDao dao = DaoRegistry.forClass(UserDao.class, dataSource);
        dao.insertUser(new User("1", "John", "john@test.com"));

        List<User> results = dao.findByFilter(new UserFilter("Nobody", "x@test.com"));
        assertTrue(results.isEmpty());
    }

    // --- Condition tests ---

    @Test
    void conditionDynamic() {
        // Test Condition builder API
        ICondition cond = Condition.and(
                Condition.eq("active", true),
                Condition.like("name", null),   // skipped
                Condition.gt("age", 18)
        );
        assertEquals("(active = ? AND age > ?)", cond.sql());
        assertEquals(List.of(true, 18), cond.params());
    }

    @Test
    void conditionEmpty() {
        ICondition cond = Condition.and(
                Condition.eq("a", null),
                Condition.like("b", null)
        );
        assertEquals("", cond.sql());
        assertEquals(List.of(), cond.params());
    }

    @Test
    void conditionIn() {
        ICondition cond = Condition.in("id", List.of(1, 2, 3));
        assertEquals("id IN (?,?,?)", cond.sql());
        assertEquals(List.of(1, 2, 3), cond.params());
    }

    @Test
    void conditionBetween() {
        ICondition cond = Condition.between("age", 18, 65);
        assertEquals("age BETWEEN ? AND ?", cond.sql());
        assertEquals(List.of(18, 65), cond.params());
    }

    @Test
    void conditionOr() {
        ICondition cond = Condition.or(
                Condition.eq("status", "active"),
                Condition.eq("status", "pending")
        );
        assertEquals("(status = ? OR status = ?)", cond.sql());
        assertEquals(List.of("active", "pending"), cond.params());
    }

    @Test
    void conditionNot() {
        ICondition cond = Condition.not(Condition.eq("deleted", true));
        assertEquals("NOT (deleted = ?)", cond.sql());
        assertEquals(List.of(true), cond.params());
    }

    // --- Spread tests ---

    @Test
    void spreadVarargs() {
        Spread<Integer> spread = Spread.of(1, 2, 3);
        assertEquals(List.of(1, 2, 3), spread.values());
    }

    @Test
    void spreadCollection() {
        Spread<String> spread = Spread.of(List.of("a", "b"));
        assertEquals(List.of("a", "b"), spread.values());
    }

    @Test
    void spreadEmptyThrows() {
        assertThrows(DaoException.class, () -> Spread.of(List.of()));
    }
}
