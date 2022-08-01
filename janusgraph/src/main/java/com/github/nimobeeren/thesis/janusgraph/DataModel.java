package com.github.nimobeeren.thesis.janusgraph;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

public abstract class DataModel {
  JanusGraph graph;

  DataModel(JanusGraph graph) {
    this.graph = graph;
  }

  public void load(File dataDir) throws Exception {
    loadSchema();
    loadData(dataDir);
  }

  abstract void loadSchema();

  abstract void loadData(File dataDir) throws Exception;

  abstract void mangle();

  abstract void mangleSingle();

  abstract GraphTraversal<Vertex, Vertex> findViolatingVertices();

  abstract GraphTraversal<Edge, Edge> findViolatingEdges();

  Set<Element> validate() {
    Set<Element> violatingElements = new HashSet<Element>();
    violatingElements.addAll(findViolatingVertices().toSet());
    violatingElements.addAll(findViolatingEdges().toSet());
    return violatingElements;
  }

  boolean validateBoolean() {
    return !(findViolatingVertices().hasNext() || findViolatingEdges().hasNext());
  }
}
