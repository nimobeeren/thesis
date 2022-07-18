package com.github.nimobeeren.thesis.janusgraph;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasNot;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
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

public class SNBModel extends DataModel {
  SNBModel(JanusGraph graph) {
    super(graph);
  }

  public void loadSchema() {
    JanusGraphManagement mgmt = graph.openManagement();

    // Vertex labels
    VertexLabel Comment = mgmt.makeVertexLabel("Comment").make();
    VertexLabel Post = mgmt.makeVertexLabel("Post").make();
    VertexLabel Company = mgmt.makeVertexLabel("Company").make();
    VertexLabel University = mgmt.makeVertexLabel("University").make();
    VertexLabel City = mgmt.makeVertexLabel("City").make();
    VertexLabel Country = mgmt.makeVertexLabel("Country").make();
    VertexLabel Continent = mgmt.makeVertexLabel("Continent").make();
    VertexLabel Forum = mgmt.makeVertexLabel("Forum").make();
    VertexLabel Person = mgmt.makeVertexLabel("Person").make();
    VertexLabel Tag = mgmt.makeVertexLabel("Tag").make();
    VertexLabel TagClass = mgmt.makeVertexLabel("TagClass").make();

    // Property keys with datatypes and cardinalities
    PropertyKey creation_dateKey = mgmt.makePropertyKey("creation_date").dataType(Date.class).make();
    PropertyKey location_ipKey = mgmt.makePropertyKey("location_ip").dataType(String.class).make();
    PropertyKey browser_usedKey = mgmt.makePropertyKey("browser_used").dataType(String.class).make();
    PropertyKey contentKey = mgmt.makePropertyKey("content").dataType(String.class).make();
    PropertyKey lengthKey = mgmt.makePropertyKey("length").dataType(Integer.class).make();
    PropertyKey image_fileKey = mgmt.makePropertyKey("image_file").dataType(String.class).make();
    PropertyKey langKey = mgmt.makePropertyKey("lang").dataType(String.class).make();
    PropertyKey nameKey = mgmt.makePropertyKey("name").dataType(String.class).make();
    PropertyKey urlKey = mgmt.makePropertyKey("url").dataType(String.class).make();
    PropertyKey titleKey = mgmt.makePropertyKey("title").dataType(String.class).make();
    PropertyKey first_nameKey = mgmt.makePropertyKey("first_name").dataType(String.class).make();
    PropertyKey last_nameKey = mgmt.makePropertyKey("last_name").dataType(String.class).make();
    PropertyKey genderKey = mgmt.makePropertyKey("gender").dataType(String.class).make();
    PropertyKey birthdayKey = mgmt.makePropertyKey("birthday").dataType(Date.class).make();
    PropertyKey speaksKey = mgmt.makePropertyKey("speaks").dataType(String.class).cardinality(Cardinality.SET).make();
    PropertyKey emailKey = mgmt.makePropertyKey("email").dataType(String.class).cardinality(Cardinality.SET).make();
    PropertyKey classYearKey = mgmt.makePropertyKey("classYear").dataType(Integer.class).make();
    PropertyKey workFromKey = mgmt.makePropertyKey("workFrom").dataType(Integer.class).make();

    // Vertex properties
    // mgmt.addProperties(Movie, budgetKey, countriesKey, imdbIdKey, imdbRatingKey, imdbVotesKey,
    //     languagesKey, movieIdKey, plotKey, posterKey, releasedKey, revenueKey, runtimeKey, titleKey,
    //     tmdbIdKey, urlKey, yearKey);

    // Edge labels and connections
    // EdgeLabel ACTED_IN = mgmt.makeEdgeLabel("ACTED_IN").multiplicity(Multiplicity.SIMPLE).make();
    // mgmt.addConnection(ACTED_IN, Actor, Movie);

    // Edge properties
    // mgmt.addProperties(ACTED_IN, roleKey);

    // Set file path for all schema elements
    // filePathByVertex.put("Movie", "movies.csv");
    // filePathByEdge.put("ACTED_IN", "actedIn.csv");

    mgmt.commit();
  }

  boolean validateBoolean() {
    throw new NotImplementedException();
  }

  Set<Element> validate() {
    throw new NotImplementedException();
  }

  public void loadData(File dataDir) throws IOException, ParseException {
    
  }
}
