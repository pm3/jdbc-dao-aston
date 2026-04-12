package eu.aston.dao;

/**
 * Entity configuration — maps a bean/record class to a database table.
 */
public final class EntityConfig<T> {

    private final Class<T> type;
    private final String table;
    private final String pk;
    private final String createdAt;
    private final String updatedAt;

    private EntityConfig(Class<T> type, String table, String pk, String createdAt, String updatedAt) {
        this.type = type;
        this.table = table;
        this.pk = pk;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Class<T> type() { return type; }
    public String table() { return table; }
    public String pk() { return pk; }
    public String createdAt() { return createdAt; }
    public String updatedAt() { return updatedAt; }

    public static <T> Builder<T> of(Class<T> type, String table) {
        return new Builder<>(type, table);
    }

    public static final class Builder<T> {
        private final Class<T> type;
        private final String table;
        private String pk = "id";
        private String createdAt;
        private String updatedAt;

        private Builder(Class<T> type, String table) {
            this.type = type;
            this.table = table;
        }

        public Builder<T> pk(String pk) { this.pk = pk; return this; }
        public Builder<T> createdAt(String createdAt) { this.createdAt = createdAt; return this; }
        public Builder<T> updatedAt(String updatedAt) { this.updatedAt = updatedAt; return this; }

        public EntityConfig<T> build() {
            return new EntityConfig<>(type, table, pk, createdAt, updatedAt);
        }
    }
}
