package com.github.nimobeeren.thesis.janusgraph;

import java.io.File;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.Element;
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

  abstract boolean validateBoolean();

  abstract Set<Element> validate();

  abstract void loadData(File dataDir) throws Exception;
}
