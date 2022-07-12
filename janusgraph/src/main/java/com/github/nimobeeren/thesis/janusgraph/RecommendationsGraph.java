package com.github.nimobeeren.thesis.janusgraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;

public class RecommendationsGraph {

  private String dirPath;
  private CSVFormat parser;

  public RecommendationsGraph(String dirPath) {
    this.dirPath = dirPath;
    this.parser =
        CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true).setNullString("").build();
  }

  private Iterable<CSVRecord> parseFile(String fileName) throws FileNotFoundException, IOException {
    return parser.parse(new FileReader(new File(dirPath, fileName)));
  }

  public void load(JanusGraph graph) throws FileNotFoundException, IOException {
    Iterable<CSVRecord> movies = parseFile("movies.csv");

    for (CSVRecord movieRecord : movies) {
      JanusGraphVertex movieVertex = graph.addVertex("Movie");
      movieVertex.property("budget", movieRecord.get("budget"));
      movieVertex.property("countries", movieRecord.get("countries"));
      movieVertex.property("imdbId", movieRecord.get("imdbId"));
      movieVertex.property("imdbRating", movieRecord.get("imdbRating"));
      movieVertex.property("imdbVotes", movieRecord.get("imdbVotes"));
      movieVertex.property("languages", movieRecord.get("languages"));
      movieVertex.property("movieId", movieRecord.get("movieId"));
      movieVertex.property("plot", movieRecord.get("plot"));
      movieVertex.property("poster", movieRecord.get("poster"));
      movieVertex.property("released", movieRecord.get("released"));
      movieVertex.property("revenue", movieRecord.get("revenue"));
      movieVertex.property("runtime", movieRecord.get("runtime"));
      movieVertex.property("title", movieRecord.get("title"));
      movieVertex.property("tmdbId", movieRecord.get("tmdbId"));
      movieVertex.property("url", movieRecord.get("url"));
      movieVertex.property("year", movieRecord.get("year"));
    }

    graph.tx().commit();
  }
}
