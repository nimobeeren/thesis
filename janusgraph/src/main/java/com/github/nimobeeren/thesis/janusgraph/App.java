package com.github.nimobeeren.thesis.janusgraph;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

// FIXME: The JanusGraph classes cannot be found when running in the container, either
// have to add them to classpath manually (which won't work for the CSV library)
// or package them in a jar

public class App {
    public static void main(String[] args) {
        JanusGraphFactory.Builder config = JanusGraphFactory.build();
        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", "var/lib/janusgraph/data");
        JanusGraph graph = config.open();

        System.out.println(graph.traversal().V());
    }
}
