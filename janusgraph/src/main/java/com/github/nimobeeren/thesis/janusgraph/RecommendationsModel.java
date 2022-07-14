package com.github.nimobeeren.thesis.janusgraph;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasNot;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
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

public class RecommendationsModel {

  Iterable<CSVRecord> parseFile(File dir, String fileName) throws IOException {
    CSVFormat parser =
        CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true).setNullString("").build();
    return parser.parse(new FileReader(new File(dir, fileName)));
  }

  Long parseId(IDManager idManager, String idString) throws ParseException {
    Long longId = Long.parseLong(idString);
    if (longId == 0) {
      // HACK: IDs must be positive, so let's try this instead and hope no vertex has that ID
      return idManager.toVertexId(999999999l);
    } else {
      return idManager.toVertexId(longId);
    }
  }

  public void load(JanusGraph graph, File dataDir) throws IOException, ParseException {
    loadSchema(graph);
    loadData(graph, dataDir);
  }

  public void loadSchema(JanusGraph graph) {
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
    PropertyKey genreKey = mgmt.makePropertyKey("genre").dataType(String.class).make();
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
    mgmt.addProperties(Genre, genreKey);

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

    mgmt.commit();
  }

  public void loadData(JanusGraph graph, File dataDir) throws IOException, ParseException {
    JanusGraphTransaction tx = graph.buildTransaction().enableBatchLoading().start();
    IDManager idManager = ((StandardJanusGraph) graph).getIDManager();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /* VERTICES */

    VertexLabel Movie = tx.getVertexLabel("Movie");
    VertexLabel Actor = tx.getVertexLabel("Actor");
    VertexLabel Director = tx.getVertexLabel("Director");
    VertexLabel ActorDirector = tx.getVertexLabel("ActorDirector");
    VertexLabel User = tx.getVertexLabel("User");
    VertexLabel Genre = tx.getVertexLabel("Genre");

    Iterable<CSVRecord> movies = parseFile(dataDir, "movies.csv");

    for (CSVRecord movieRecord : movies) {
      Long vertexId = parseId(idManager, movieRecord.get("_id"));
      JanusGraphVertex movieVertex = tx.addVertex(vertexId, Movie);
      movieVertex.property("budget", movieRecord.get("budget"));
      if (movieRecord.get("countries") != null) {
        for (String country : movieRecord.get("countries").replaceAll("[\\[\\]\"]", "")
            .split(",")) {
          movieVertex.property("countries", country);
        }
      }
      movieVertex.property("imdbId", movieRecord.get("imdbId"));
      movieVertex.property("imdbRating", movieRecord.get("imdbRating"));
      movieVertex.property("imdbVotes", movieRecord.get("imdbVotes"));
      if (movieRecord.get("languages") != null) {
        for (String language : movieRecord.get("languages").replaceAll("[\\[\\]\"]", "")
            .split(",")) {
          movieVertex.property("languages", language);
        }
      }
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

    Iterable<CSVRecord> actors = parseFile(dataDir, "actors.csv");

    for (CSVRecord actorRecord : actors) {
      Long vertexId = parseId(idManager, actorRecord.get("_id"));
      JanusGraphVertex actorVertex = tx.addVertex(vertexId, Actor);
      actorVertex.property("bio", actorRecord.get("bio"));
      if (actorRecord.get("born") != null) {
        actorVertex.property("born", dateFormat.parse(actorRecord.get("born")));
      }
      actorVertex.property("bornIn", actorRecord.get("bornIn"));
      if (actorRecord.get("died") != null) {
        actorVertex.property("died", dateFormat.parse(actorRecord.get("died")));
      }
      actorVertex.property("imdbId", actorRecord.get("imdbId"));
      actorVertex.property("name", actorRecord.get("name"));
      actorVertex.property("poster", actorRecord.get("poster"));
      actorVertex.property("tmdbId", actorRecord.get("tmdbId"));
      actorVertex.property("url", actorRecord.get("url"));
    }

    Iterable<CSVRecord> directors = parseFile(dataDir, "directors.csv");

    for (CSVRecord directorRecord : directors) {
      Long vertexId = parseId(idManager, directorRecord.get("_id"));
      JanusGraphVertex directorVertex = tx.addVertex(vertexId, Director);
      directorVertex.property("bio", directorRecord.get("bio"));
      if (directorRecord.get("born") != null) {
        directorVertex.property("born", dateFormat.parse(directorRecord.get("born")));
      }
      directorVertex.property("bornIn", directorRecord.get("bornIn"));
      if (directorRecord.get("died") != null) {
        directorVertex.property("died", dateFormat.parse(directorRecord.get("died")));
      }
      directorVertex.property("imdbId", directorRecord.get("imdbId"));
      directorVertex.property("name", directorRecord.get("name"));
      directorVertex.property("poster", directorRecord.get("poster"));
      directorVertex.property("tmdbId", directorRecord.get("tmdbId"));
      directorVertex.property("url", directorRecord.get("url"));
    }

    Iterable<CSVRecord> actorDirectors = parseFile(dataDir, "actorDirectors.csv");

    for (CSVRecord actorDirectorRecord : actorDirectors) {
      Long vertexId = parseId(idManager, actorDirectorRecord.get("_id"));
      JanusGraphVertex actorDirectorVertex = tx.addVertex(vertexId, ActorDirector);
      actorDirectorVertex.property("bio", actorDirectorRecord.get("bio"));
      if (actorDirectorRecord.get("born") != null) {
        actorDirectorVertex.property("born", dateFormat.parse(actorDirectorRecord.get("born")));
      }
      actorDirectorVertex.property("bornIn", actorDirectorRecord.get("bornIn"));
      if (actorDirectorRecord.get("died") != null) {
        actorDirectorVertex.property("died", dateFormat.parse(actorDirectorRecord.get("died")));
      }
      actorDirectorVertex.property("imdbId", actorDirectorRecord.get("imdbId"));
      actorDirectorVertex.property("name", actorDirectorRecord.get("name"));
      actorDirectorVertex.property("poster", actorDirectorRecord.get("poster"));
      actorDirectorVertex.property("tmdbId", actorDirectorRecord.get("tmdbId"));
      actorDirectorVertex.property("url", actorDirectorRecord.get("url"));
    }

    Iterable<CSVRecord> users = parseFile(dataDir, "users.csv");

    for (CSVRecord userRecord : users) {
      Long vertexId = parseId(idManager, userRecord.get("_id"));
      JanusGraphVertex userVertex = tx.addVertex(vertexId, User);
      userVertex.property("name", userRecord.get("name"));
      userVertex.property("userId", userRecord.get("userId"));
    }

    Iterable<CSVRecord> genres = parseFile(dataDir, "genres.csv");

    for (CSVRecord genreRecord : genres) {
      Long vertexId = parseId(idManager, genreRecord.get("_id"));
      JanusGraphVertex genreVertex = tx.addVertex(vertexId, Genre);
      genreVertex.property("genre", genreRecord.get("genre"));
    }

    /* EDGES */

    Iterable<CSVRecord> actedIn = parseFile(dataDir, "actedIn.csv");

    for (CSVRecord actedInRecord : actedIn) {
      Long startId = parseId(idManager, actedInRecord.get("_start"));
      Long endId = parseId(idManager, actedInRecord.get("_end"));
      Iterator<Vertex> vertices = tx.vertices(startId, endId);
      Vertex start = vertices.next();
      Vertex end = vertices.next();
      Edge edge = start.addEdge("ACTED_IN", end);
      edge.property("role", actedInRecord.get("role"));
    }

    Iterable<CSVRecord> directed = parseFile(dataDir, "directed.csv");

    for (CSVRecord directedRecord : directed) {
      Long startId = parseId(idManager, directedRecord.get("_start"));
      Long endId = parseId(idManager, directedRecord.get("_end"));
      Iterator<Vertex> vertices = tx.vertices(startId, endId);
      Vertex start = vertices.next();
      Vertex end = vertices.next();
      Edge edge = start.addEdge("DIRECTED", end);
      edge.property("role", directedRecord.get("role"));
    }

    Iterable<CSVRecord> inGenre = parseFile(dataDir, "inGenre.csv");

    for (CSVRecord inGenreRecord : inGenre) {
      Long startId = parseId(idManager, inGenreRecord.get("_start"));
      Long endId = parseId(idManager, inGenreRecord.get("_end"));
      Iterator<Vertex> vertices = tx.vertices(startId, endId);
      Vertex start = vertices.next();
      Vertex end = vertices.next();
      start.addEdge("IN_GENRE", end);
    }

    Iterable<CSVRecord> rated = parseFile(dataDir, "rated.csv");

    for (CSVRecord ratedRecord : rated) {
      Long startId = parseId(idManager, ratedRecord.get("_start"));
      Long endId = parseId(idManager, ratedRecord.get("_end"));
      Iterator<Vertex> vertices = tx.vertices(startId, endId);
      Vertex start = vertices.next();
      Vertex end = vertices.next();
      Edge edge = start.addEdge("RATED", end);
      edge.property("rating", ratedRecord.get("rating"));
      edge.property("timestamp", ratedRecord.get("timestamp"));
    }

    tx.commit();
  }

  public boolean validate(JanusGraph graph) {
    GraphTraversalSource g = graph.traversal();

    // Objects can't have labels that are not allowed (because automatic schema is disabled)
    // Property values can't have the wrong datatype (because of PropertyKey.dataType)
    // Objects can't have properties that are not allowed (because of addProperties)
    // Edges can't connect the wrong types of nodes (because of addConnection)

    // Check for missing mandatory properties
    if (g.V().hasLabel("Movie").or(hasNot("imdbId"), hasNot("movieId"), hasNot("title")).hasNext())
      return false;
    if (g.V().hasLabel(P.within("Actor", "Director", "ActorDirector"))
        .or(hasNot("name"), hasNot("tmdbId"), hasNot("url")).hasNext())
      return false;
    if (g.V().hasLabel("User").or(hasNot("name"), hasNot("userId")).hasNext())
      return false;
    if (g.V().hasLabel("Genre").hasNot("genre").hasNext())
      return false;
    if (g.E().hasLabel("RATED").or(hasNot("rating"), hasNot("timestamp")).hasNext())
      return false;

    // Check for missing mandatory edges
    if (g.V().hasLabel(P.within("Actor", "ActorDirector")).not(out("ACTED_IN")).hasNext())
      return false;
    if (g.V().hasLabel(P.within("Director", "ActorDirector")).not(out("DIRECTED")).hasNext())
      return false;
    if (g.V().hasLabel("Movie").not(out("IN_GENRE")).hasNext())
      return false;

    return true;
  }
}