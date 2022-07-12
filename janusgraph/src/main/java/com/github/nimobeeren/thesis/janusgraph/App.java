package com.github.nimobeeren.thesis.janusgraph;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class App {
    public static void main(String[] args) {
        JanusGraphFactory.Builder config = JanusGraphFactory.build();
        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", "/var/lib/janusgraph/data");
        JanusGraph graph = config.open();

        graph.addVertex("Person").property("hello", "world");
        graph.tx().commit();

        System.exit(0);
    }
}
