package eu.aston.beanmeta;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReflectiveBeanMetaTest {

    @BeforeEach
    void reset() {
        BeanMetaRegistry.reset();
    }

    // --- Record tests ---

    public record PersonRecord(String name, int age, List<String> tags) {
    }

    @Test
    void recordMeta_type() {
        BeanMeta<PersonRecord> meta = BeanMetaRegistry.forClass(PersonRecord.class);
        assertEquals(PersonRecord.class, meta.type());
    }

    @Test
    void recordMeta_names() {
        BeanMeta<PersonRecord> meta = BeanMetaRegistry.forClass(PersonRecord.class);
        assertEquals(List.of("name", "age", "tags"), meta.names());
    }

    @Test
    void recordMeta_types() {
        BeanMeta<PersonRecord> meta = BeanMetaRegistry.forClass(PersonRecord.class);
        assertEquals(List.of(String.class, int.class, List.class), meta.types());
    }

    @Test
    void recordMeta_genericTypes() {
        BeanMeta<PersonRecord> meta = BeanMetaRegistry.forClass(PersonRecord.class);
        List<Type> genericTypes = meta.genericTypes();
        assertEquals(3, genericTypes.size());
        assertEquals(String.class, genericTypes.get(0));
        assertEquals(int.class, genericTypes.get(1));
        // third should be List<String>
        assertInstanceOf(ParameterizedType.class, genericTypes.get(2));
        ParameterizedType pt = (ParameterizedType) genericTypes.get(2);
        assertEquals(List.class, pt.getRawType());
        assertEquals(String.class, pt.getActualTypeArguments()[0]);
    }

    @Test
    void recordMeta_get() {
        BeanMeta<PersonRecord> meta = BeanMetaRegistry.forClass(PersonRecord.class);
        PersonRecord person = new PersonRecord("John", 30, List.of("dev"));
        assertEquals("John", meta.get(person, "name"));
        assertEquals(30, meta.get(person, "age"));
        assertEquals(List.of("dev"), meta.get(person, "tags"));
    }

    @Test
    void recordMeta_get_unknownProperty() {
        BeanMeta<PersonRecord> meta = BeanMetaRegistry.forClass(PersonRecord.class);
        PersonRecord person = new PersonRecord("John", 30, List.of());
        assertThrows(IllegalArgumentException.class, () -> meta.get(person, "unknown"));
    }

    @Test
    void recordMeta_create() {
        BeanMeta<PersonRecord> meta = BeanMetaRegistry.forClass(PersonRecord.class);
        PersonRecord person = meta.create("Jane", 25, List.of("qa"));
        assertEquals("Jane", person.name());
        assertEquals(25, person.age());
        assertEquals(List.of("qa"), person.tags());
    }

    // --- JavaBean with getters/setters tests ---

    public static class AddressBean {
        private String street;
        private String city;

        public AddressBean() {
        }

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }

    @Test
    void beanMeta_names() {
        BeanMeta<AddressBean> meta = BeanMetaRegistry.forClass(AddressBean.class);
        assertTrue(meta.names().containsAll(List.of("street", "city")));
        assertEquals(2, meta.names().size());
    }

    @Test
    void beanMeta_get() {
        BeanMeta<AddressBean> meta = BeanMetaRegistry.forClass(AddressBean.class);
        AddressBean addr = new AddressBean();
        addr.setStreet("Main St");
        addr.setCity("Springfield");
        assertEquals("Main St", meta.get(addr, "street"));
        assertEquals("Springfield", meta.get(addr, "city"));
    }

    @Test
    void beanMeta_create() {
        BeanMeta<AddressBean> meta = BeanMetaRegistry.forClass(AddressBean.class);
        // create uses setter-based approach (no-arg ctor + setters)
        // order matches names()
        List<String> names = meta.names();
        Object[] values = new Object[names.size()];
        for (int i = 0; i < names.size(); i++) {
            values[i] = switch (names.get(i)) {
                case "street" -> "Oak Ave";
                case "city" -> "Denver";
                default -> null;
            };
        }
        AddressBean addr = meta.create(values);
        assertEquals("Oak Ave", addr.getStreet());
        assertEquals("Denver", addr.getCity());
    }

    // --- ParameterizedTypeImpl tests ---

    @Test
    void parameterizedTypeImpl_equality() {
        var pt1 = new ParameterizedTypeImpl(List.class, new Type[] { String.class });
        var pt2 = new ParameterizedTypeImpl(List.class, new Type[] { String.class });
        assertEquals(pt1, pt2);
        assertEquals(pt1.hashCode(), pt2.hashCode());
    }

    @Test
    void parameterizedTypeImpl_toString() {
        var pt = new ParameterizedTypeImpl(List.class, new Type[] { String.class });
        assertEquals("java.util.List<java.lang.String>", pt.toString());
    }

    // --- Caching test ---

    @Test
    void registry_caches() {
        BeanMeta<PersonRecord> meta1 = BeanMetaRegistry.forClass(PersonRecord.class);
        BeanMeta<PersonRecord> meta2 = BeanMetaRegistry.forClass(PersonRecord.class);
        assertSame(meta1, meta2);
    }

    @Test
    void registry_noCompiledMeta() {
        assertFalse(BeanMetaRegistry.hasCompiledMeta(PersonRecord.class));
    }
}
