package eu.aston.dao.test;

import eu.aston.dao.DaoApi;
import eu.aston.dao.EntityConfig;
import eu.aston.dao.Query;

import java.util.List;

@DaoApi
public interface TestProductDao {

    EntityConfig<TestProduct> PRODUCT = EntityConfig.of(TestProduct.class, "products").createdAt("").updatedAt("")
            .build();

    void insertProduct(TestProduct product);

    TestProduct loadById(String id);

    @Query("SELECT * FROM products")
    List<TestProduct> findAll();
}
