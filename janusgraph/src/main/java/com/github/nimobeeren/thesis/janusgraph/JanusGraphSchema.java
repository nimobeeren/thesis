package com.github.nimobeeren.thesis.janusgraph;

import java.io.File;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "janusgraph-schema", mixinStandardHelpOptions = true,
    version = "janusgraph-schema 0.1", subcommands = CommandLine.HelpCommand.class)
public class JanusGraphSchema {

  private JanusGraphFactory.Builder graphConfig;

  public JanusGraphSchema() {
    this.graphConfig = JanusGraphFactory.build();
    this.graphConfig.set("storage.backend", "berkeleyje");
    this.graphConfig.set("storage.directory", "/var/lib/janusgraph/data");
    this.graphConfig.set("schema.default", "none"); // disable automatic schema generation in favor
                                                    // of explicit schema
    this.graphConfig.set("schema.constraints", "true"); // enable property and edge connection
                                                        // constraints
    this.graphConfig.set("graph.set-vertex-id", "true");
  }

  @Command(name = "load")
  void load(@Parameters(paramLabel = "dataDir",
      description = "Path to a directory containing all CSV files needed for data loading") File dataDir,
      @Option(names = {"-D", "--drop"}) boolean shouldDrop) throws Exception {
    System.out.println("Opening graph...");
    JanusGraph graph = graphConfig.open();

    if (shouldDrop) {
      System.out.println("Dropping graph...");
      JanusGraphFactory.drop(graph);
      graph = graphConfig.open();
    }

    System.out.println("Loading graph...");
    RecommendationsModel recommendations = new RecommendationsModel(dataDir.toPath().toString());
    recommendations.load(graph);

    System.out.println("Done");
  }

  @Command(name = "validate")
  void validate() {
    System.out.println("Opening graph...");
    JanusGraph graph = graphConfig.open();

    // FIXME: requires dataDir argument even if we're not loading data
    System.out.println("Validating...");
    RecommendationsModel recommendations = new RecommendationsModel(null);
    boolean isValid = recommendations.validate(graph);
    if (isValid) {
      System.out.println("Graph conforms to schema ✅");
    } else {
      System.out.println("Graph does not conform to schema ❌");
    }
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
