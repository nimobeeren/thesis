package com.github.nimobeeren.thesis.janusgraph;

import java.io.File;
import java.util.Set;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "janusgraph-schema", mixinStandardHelpOptions = true, version = "0.1",
    subcommands = CommandLine.HelpCommand.class)
public class JanusGraphSchema {

  JanusGraphFactory.Builder graphConfig;

  enum Dataset {
    recommendations, snb
  }

  public JanusGraphSchema() {
    this.graphConfig = JanusGraphFactory.build();
    this.graphConfig.set("storage.backend", "berkeleyje");
    this.graphConfig.set("storage.directory", "/var/lib/janusgraph/data");
    this.graphConfig.set("schema.default", "none"); // disable automatic schema generation in favor
                                                    // of explicit schema
    this.graphConfig.set("schema.constraints", "true"); // enable property and edge connection
                                                        // constraints
  }

  @Command
  void load(
      @Parameters(paramLabel = "dataset",
          description = "One of the dataset names: ${COMPLETION-CANDIDATES}") Dataset dataset,
      @Parameters(paramLabel = "path",
          description = "Path to a directory containing all required CSV files") File path,
      @Option(names = {"-D", "--drop"},
          description = {"Drop all existing data"}) boolean shouldDrop)
      throws Exception {

    System.out.println("Opening graph...");
    if (dataset == Dataset.recommendations) {
      // Enable manual setting of IDs, because this dataset contains globally unique IDs
      graphConfig.set("graph.set-vertex-id", "true");
    }
    JanusGraph graph = graphConfig.open();

    if (shouldDrop) {
      System.out.println("Dropping graph...");
      JanusGraphFactory.drop(graph);
      graph = graphConfig.open();
    }

    System.out.println("Loading graph...");
    DataModel model;
    switch (dataset) {
      case recommendations:
        model = new RecommendationsModel(graph);
        break;
      case snb:
        model = new SNBModel(graph);
        break;
      default:
        throw new NotImplementedException();
    }
    model.load(path);

    System.out.println("Done");
  }

  @Command
  void validate(
      @Parameters(paramLabel = "dataset",
          description = "One of the dataset names: ${COMPLETION-CANDIDATES}") Dataset dataset,
      @Option(names = {"-b", "--boolean"},
          description = "Only check whether the graph conforms to the schema or not. This is faster than enumerating all the violating elements") boolean validateBoolean) {
    System.out.println("Opening graph...");
    JanusGraph graph = graphConfig.open();

    System.out.println("Validating...");
    DataModel model;
    switch (dataset) {
      case recommendations:
        model = new RecommendationsModel(graph);
        break;
      case snb:
        throw new NotImplementedException();
      default:
        throw new NotImplementedException();
    }
    long startTime = System.currentTimeMillis();
    if (validateBoolean) {
      boolean isValid = model.validateBoolean();
      if (isValid) {
        System.out.println("Graph conforms to schema ✅");
      } else {
        System.out.println("Graph does not conform to schema ❌");
      }
    } else {
      Set<Element> violatingElements = model.validate();
      if (violatingElements.size() == 0) {
        System.out.println("All graph elements conform to schema ✅");
      } else {
        System.out.println(
            String.format("%d elements do not conform to schema ❌", violatingElements.size()));
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println(String.format("Took %d ms", endTime - startTime));
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = new CommandLine(new JanusGraphSchema());
    if (args.length == 0) {
      cmd.usage(System.out);
      System.exit(1);
    } else {
      int exitCode = cmd.execute(args);
      System.exit(exitCode);
    }
  }
}
