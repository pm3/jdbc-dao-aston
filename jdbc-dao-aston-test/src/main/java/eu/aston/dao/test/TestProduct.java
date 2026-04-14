package eu.aston.dao.test;

import eu.aston.beanmeta.GenerateMeta;

import java.util.Map;

@GenerateMeta
public record TestProduct(String id, String name, double price, Map<String, String> attrs) {
}
