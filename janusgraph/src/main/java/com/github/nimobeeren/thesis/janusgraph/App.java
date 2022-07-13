package com.github.nimobeeren.thesis.janusgraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;

public class App {
  public static void main(String[] args)
      throws FileNotFoundException, IOException, BackendException, ParseException {
    JanusGraphFactory.Builder config = JanusGraphFactory.build();
    config.set("storage.backend", "berkeleyje");
    config.set("storage.directory", "/var/lib/janusgraph/data");
    config.set("schema.default", "none"); // disable automatic schema generation in favor of
                                          // explicit schema
    config.set("schema.constraints", "true"); // enable property and edge connection constraints
    JanusGraph graph = config.open();

    System.out.println("Dropping graph...");
    JanusGraphFactory.drop(graph);

    System.out.println("Opening graph...");
    graph = config.open();

    System.out.println("Loading graph...");
    RecommendationsGraph recommendations = new RecommendationsGraph(args[0]);
    recommendations.load(graph);

    System.out.println("Done");
    System.exit(0);
  }
}
