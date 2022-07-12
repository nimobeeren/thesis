package com.github.nimobeeren.thesis.janusgraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class App {
  public static void main(String[] args) throws FileNotFoundException, IOException {
    JanusGraphFactory.Builder config = JanusGraphFactory.build();
    config.set("storage.backend", "berkeleyje");
    config.set("storage.directory", "/var/lib/janusgraph/data");
    
    JanusGraph graph = config.open();
    System.out.println("Opened graph");

    RecommendationsGraph recommendations = new RecommendationsGraph(args[0]);
    recommendations.load(graph);
    System.out.println("Loaded graph");

    System.exit(0);
  }
}
