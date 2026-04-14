package eu.aston.dao.test;

import eu.aston.dao.DaoApi;
import eu.aston.dao.EntityConfig;
import eu.aston.dao.Query;

import java.util.List;
import java.util.Optional;

@DaoApi
public interface TestUserDao {

    EntityConfig<TestUser> USER = EntityConfig.of(TestUser.class, "users").createdAt("createdat").build();

    TestUser loadById(String id);

    void insertUser(TestUser user);

    void updateUser(TestUser user);

    void deleteUser(TestUser user);

    @Query("SELECT * FROM users WHERE email=:email")
    Optional<TestUser> findByEmail(String email);

    @Query("SELECT * FROM users")
    List<TestUser> findAll();

    @Query("SELECT count(*) FROM users")
    long countAll();

    @Query("DELETE FROM users")
    void deleteAll();
}
