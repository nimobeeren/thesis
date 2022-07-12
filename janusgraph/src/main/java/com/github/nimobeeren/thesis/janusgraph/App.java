package com.github.nimobeeren.thesis.janusgraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;

public class App {
  public static void main(String[] args) throws FileNotFoundException, IOException, BackendException {
    JanusGraphFactory.Builder config = JanusGraphFactory.build();
    config.set("storage.backend", "berkeleyje");
    config.set("storage.directory", "/var/lib/janusgraph/data");
    config.set("schema.default", "none");
    JanusGraph graph = config.open();

    JanusGraphFactory.drop(graph);
    System.out.println("Dropped graph");

    graph = config.open();
    System.out.println("Opened graph");

    RecommendationsGraph recommendations = new RecommendationsGraph(args[0]);
    recommendations.load(graph);
    System.out.println("Loaded graph");

    System.exit(0);
  }
}
