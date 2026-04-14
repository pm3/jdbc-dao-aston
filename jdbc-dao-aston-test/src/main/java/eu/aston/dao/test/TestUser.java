package eu.aston.dao.test;

import eu.aston.beanmeta.GenerateMeta;

import java.time.Instant;

@GenerateMeta
public record TestUser(String id, String name, String email, boolean active, Instant createdat) {
}
