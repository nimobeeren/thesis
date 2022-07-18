package com.github.nimobeeren.thesis.janusgraph;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasNot;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

public class RecommendationsModel extends DataModel {

  RecommendationsModel(JanusGraph graph) {
    super(graph);
  }

  public void loadSchema() {
    JanusGraphManagement mgmt = graph.openManagement();

    // Vertex labels
    VertexLabel Movie = mgmt.makeVertexLabel("Movie").make();
    VertexLabel Actor = mgmt.makeVertexLabel("Actor").make();
    VertexLabel Director = mgmt.makeVertexLabel("Director").make();
    VertexLabel ActorDirector = mgmt.makeVertexLabel("ActorDirector").make();
    VertexLabel User = mgmt.makeVertexLabel("User").make();
    VertexLabel Genre = mgmt.makeVertexLabel("Genre").make();

    // Property keys with datatypes and cardinalities
    PropertyKey budgetKey = mgmt.makePropertyKey("budget").dataType(Long.class).make();
    PropertyKey countriesKey = mgmt.makePropertyKey("countries").dataType(String.class)
        .cardinality(Cardinality.LIST).make();
    PropertyKey imdbIdKey = mgmt.makePropertyKey("imdbId").dataType(String.class).make();
    PropertyKey imdbRatingKey = mgmt.makePropertyKey("imdbRating").dataType(Float.class).make();
    PropertyKey imdbVotesKey = mgmt.makePropertyKey("imdbVotes").dataType(Long.class).make();
    PropertyKey languagesKey = mgmt.makePropertyKey("languages").dataType(String.class)
        .cardinality(Cardinality.LIST).make();
    PropertyKey movieIdKey = mgmt.makePropertyKey("movieId").dataType(String.class).make();
    PropertyKey plotKey = mgmt.makePropertyKey("plot").dataType(String.class).make();
    PropertyKey posterKey = mgmt.makePropertyKey("poster").dataType(String.class).make();
    PropertyKey releasedKey = mgmt.makePropertyKey("released").dataType(String.class).make();
    PropertyKey revenueKey = mgmt.makePropertyKey("revenue").dataType(Long.class).make();
    PropertyKey runtimeKey = mgmt.makePropertyKey("runtime").dataType(Short.class).make();
    PropertyKey titleKey = mgmt.makePropertyKey("title").dataType(String.class).make();
    PropertyKey tmdbIdKey = mgmt.makePropertyKey("tmdbId").dataType(String.class).make();
    PropertyKey urlKey = mgmt.makePropertyKey("url").dataType(String.class).make();
    PropertyKey yearKey = mgmt.makePropertyKey("year").dataType(Short.class).make();
    PropertyKey bioKey = mgmt.makePropertyKey("bio").dataType(String.class).make();
    PropertyKey bornKey = mgmt.makePropertyKey("born").dataType(Date.class).make();
    PropertyKey bornInKey = mgmt.makePropertyKey("bornIn").dataType(String.class).make();
    PropertyKey diedKey = mgmt.makePropertyKey("died").dataType(Date.class).make();
    PropertyKey nameKey = mgmt.makePropertyKey("name").dataType(String.class).make();
    PropertyKey userIdKey = mgmt.makePropertyKey("userId").dataType(String.class).make();
    PropertyKey roleKey = mgmt.makePropertyKey("role").dataType(String.class).make();
    PropertyKey ratingKey = mgmt.makePropertyKey("rating").dataType(Float.class).make();
    PropertyKey timestampKey = mgmt.makePropertyKey("timestamp").dataType(Long.class).make();

    // Vertex properties
    mgmt.addProperties(Movie, budgetKey, countriesKey, imdbIdKey, imdbRatingKey, imdbVotesKey,
        languagesKey, movieIdKey, plotKey, posterKey, releasedKey, revenueKey, runtimeKey, titleKey,
        tmdbIdKey, urlKey, yearKey);
    mgmt.addProperties(Actor, bioKey, bornKey, bornInKey, diedKey, imdbIdKey, nameKey, posterKey,
        tmdbIdKey, urlKey);
    mgmt.addProperties(Director, bioKey, bornKey, bornInKey, diedKey, imdbIdKey, nameKey, posterKey,
        tmdbIdKey, urlKey);
    mgmt.addProperties(ActorDirector, bioKey, bornKey, bornInKey, diedKey, imdbIdKey, nameKey,
        posterKey, tmdbIdKey, urlKey);
    mgmt.addProperties(User, nameKey, userIdKey);
    mgmt.addProperties(Genre, nameKey);

    // Edge labels and connections
    EdgeLabel ACTED_IN = mgmt.makeEdgeLabel("ACTED_IN").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(ACTED_IN, Actor, Movie);
    mgmt.addConnection(ACTED_IN, ActorDirector, Movie);
    EdgeLabel DIRECTED = mgmt.makeEdgeLabel("DIRECTED").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(DIRECTED, Director, Movie);
    mgmt.addConnection(DIRECTED, ActorDirector, Movie);
    EdgeLabel RATED = mgmt.makeEdgeLabel("RATED").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(RATED, User, Movie);
    EdgeLabel IN_GENRE = mgmt.makeEdgeLabel("IN_GENRE").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(IN_GENRE, Movie, Genre);

    // Edge properties
    mgmt.addProperties(ACTED_IN, roleKey);
    mgmt.addProperties(DIRECTED, roleKey);
    mgmt.addProperties(RATED, ratingKey, timestampKey);

    // Set file path for all schema elements
    filePathByVertex.put("Movie", "movies.csv");
    filePathByVertex.put("Actor", "actors.csv");
    filePathByVertex.put("Director", "directors.csv");
    filePathByVertex.put("ActorDirector", "actorDirectors.csv");
    filePathByVertex.put("User", "users.csv");
    filePathByVertex.put("Genre", "genres.csv");
    filePathByEdge.put("ACTED_IN", "actedIn.csv");
    filePathByEdge.put("DIRECTED", "directed.csv");
    filePathByEdge.put("RATED", "rated.csv");
    filePathByEdge.put("IN_GENRE", "inGenre.csv");

    mgmt.commit();
  }

  public boolean validate() {
    GraphTraversalSource g = graph.traversal();

    // Objects can't have labels that are not allowed (because automatic schema is disabled)
    // Property values can't have the wrong datatype (because of PropertyKey.dataType)
    // Objects can't have properties that are not allowed (because of addProperties)
    // Edges can't connect the wrong types of nodes (because of addConnection)

    boolean hasViolatingVertices = g.V().or(
        // Check for missing mandatory properties on vertices
        hasLabel("Movie").or(hasNot("imdbId"), hasNot("movieId"), hasNot("title")),
        hasLabel(P.within("Actor", "Director", "ActorDirector")).or(hasNot("name"),
            hasNot("tmdbId"), hasNot("url")),
        hasLabel("User").or(hasNot("name"), hasNot("userId")), hasLabel("Genre").hasNot("name"),
        // Check for missing mandatory edges
        hasLabel(P.within("Actor", "ActorDirector")).not(out("ACTED_IN")),
        hasLabel(P.within("Director", "ActorDirector")).not(out("DIRECTED")),
        hasLabel("Movie").not(out("IN_GENRE"))).hasNext();

    if (hasViolatingVertices) {
      return false;
    }

    // Check for missing mandatory properties on edges
    boolean hasViolatingEdges =
        g.E().hasLabel("RATED").or(hasNot("rating"), hasNot("timestamp")).hasNext();

    if (hasViolatingEdges) {
      return false;
    }

    return true;
  }

  public void loadData(File dataDir) throws IOException, ParseException {
    JanusGraphTransaction tx = graph.buildTransaction().enableBatchLoading().start();
    IDManager idManager = ((StandardJanusGraph) graph).getIDManager();

    // Loop over all vertex labels
    for (String vertexLabelName : filePathByVertex.keySet()) {
      VertexLabel vertexLabel = tx.getVertexLabel(vertexLabelName);
      Iterable<CSVRecord> records = parseFile(dataDir, filePathByVertex.get(vertexLabelName));
      // Loop over all records in the data file for that vertex
      for (CSVRecord record : records) {
        Long vertexId = parseId(idManager, record.get("_id"));
        JanusGraphVertex vertex = tx.addVertex(vertexId, vertexLabel);
        // Loop over all properties that the vertex is allowed to have
        for (PropertyKey propKey : vertexLabel.mappedProperties()) {
          String rawValue = record.get(propKey.name());
          for (Object value : parsePropertyValues(propKey, rawValue)) {
            vertex.property(propKey.name(), value);
          }
        }
      }
    }

    // Loop over all edge labels
    for (String edgeLabelName : filePathByEdge.keySet()) {
      EdgeLabel edgeLabel = tx.getEdgeLabel(edgeLabelName);
      Iterable<CSVRecord> records = parseFile(dataDir, filePathByEdge.get(edgeLabelName));
      // Loop over all records in the data file for that edge
      for (CSVRecord record : records) {
        Long startId = parseId(idManager, record.get("_start"));
        Long endId = parseId(idManager, record.get("_end"));
        Iterator<Vertex> vertices = tx.vertices(startId, endId);
        Vertex start = vertices.next();
        Vertex end = vertices.next();
        Edge edge = start.addEdge(edgeLabelName, end);
        // Loop over all properties that the edge is allowed to have
        for (PropertyKey propKey : edgeLabel.mappedProperties()) {
          String rawValue = record.get(propKey.name());
          for (Object value : parsePropertyValues(propKey, rawValue)) {
            edge.property(propKey.name(), value);
          }
        }
      }
    }

    tx.commit();
  }
}
