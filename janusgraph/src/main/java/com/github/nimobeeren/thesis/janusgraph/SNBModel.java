package com.github.nimobeeren.thesis.janusgraph;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasNot;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
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

public class SNBModel extends DataModel {

  // Files containing vertex properties
  Map<String, String> filePathByVertex = new HashMap<String, String>();
  // Files containing edge properties
  Map<String, String[]> filePathsByEdge = new HashMap<String, String[]>();
  // Multi-valued properties are stored in separate files
  Map<String, String> filePathByProperty = new HashMap<String, String>();
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  // Limit amount of data to read during development
  long MAX_RECORDS_PER_FILE = Long.MAX_VALUE;
  long COMMIT_EVERY_N_RECORDS = 10000;

  SNBModel(JanusGraph graph) {
    super(graph);
  }

  Object parsePropertyValue(PropertyKey propKey, String rawValue) throws ParseException {
    if (rawValue == null) {
      return rawValue;
    }

    if (propKey.dataType() == Date.class) {
      if (propKey.name().equals("birthday")) {
        return dateFormat.parse(rawValue);
      }
      return dateTimeFormat.parse(rawValue);
    }

    return rawValue;
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
    PropertyKey idKey = mgmt.makePropertyKey("id").dataType(Long.class).make();
    PropertyKey creationDateKey = mgmt.makePropertyKey("creationDate").dataType(Date.class).make();
    PropertyKey locationIPKey = mgmt.makePropertyKey("locationIP").dataType(String.class).make();
    PropertyKey browserUsedKey = mgmt.makePropertyKey("browserUsed").dataType(String.class).make();
    PropertyKey contentKey = mgmt.makePropertyKey("content").dataType(String.class).make();
    PropertyKey lengthKey = mgmt.makePropertyKey("length").dataType(Integer.class).make();
    PropertyKey imageFileKey = mgmt.makePropertyKey("imageFile").dataType(String.class).make();
    PropertyKey languageKey = mgmt.makePropertyKey("language").dataType(String.class).make();
    PropertyKey nameKey = mgmt.makePropertyKey("name").dataType(String.class).make();
    PropertyKey urlKey = mgmt.makePropertyKey("url").dataType(String.class).make();
    PropertyKey titleKey = mgmt.makePropertyKey("title").dataType(String.class).make();
    PropertyKey firstNameKey = mgmt.makePropertyKey("firstName").dataType(String.class).make();
    PropertyKey lastNameKey = mgmt.makePropertyKey("lastName").dataType(String.class).make();
    PropertyKey genderKey = mgmt.makePropertyKey("gender").dataType(String.class).make();
    PropertyKey birthdayKey = mgmt.makePropertyKey("birthday").dataType(Date.class).make();
    // Not modeled: set must have size >= 1
    PropertyKey speaksKey =
        mgmt.makePropertyKey("speaks").dataType(String.class).cardinality(Cardinality.SET).make();
    // Not modeled: set must have size >= 1
    PropertyKey emailKey =
        mgmt.makePropertyKey("email").dataType(String.class).cardinality(Cardinality.SET).make();
    PropertyKey classYearKey = mgmt.makePropertyKey("classYear").dataType(Integer.class).make();
    PropertyKey workFromKey = mgmt.makePropertyKey("workFrom").dataType(Integer.class).make();

    // Vertex properties
    // Message
    mgmt.addProperties(Comment, idKey, browserUsedKey, creationDateKey, locationIPKey, contentKey,
        lengthKey);
    mgmt.addProperties(Post, idKey, browserUsedKey, creationDateKey, locationIPKey, contentKey,
        lengthKey, languageKey, imageFileKey);
    // Organisation
    mgmt.addProperties(Company, idKey, nameKey, urlKey);
    mgmt.addProperties(University, idKey, nameKey, urlKey);
    // Place
    mgmt.addProperties(City, idKey, nameKey, urlKey);
    mgmt.addProperties(Country, idKey, nameKey, urlKey);
    mgmt.addProperties(Continent, idKey, nameKey, urlKey);
    // Others
    mgmt.addProperties(Forum, idKey, creationDateKey, titleKey);
    mgmt.addProperties(Person, idKey, firstNameKey, lastNameKey, genderKey, birthdayKey, emailKey,
        speaksKey, browserUsedKey, locationIPKey, creationDateKey);
    mgmt.addProperties(Tag, idKey, nameKey, urlKey);
    mgmt.addProperties(TagClass, idKey, nameKey, urlKey);

    // Edge labels and connections
    EdgeLabel CONTAINER_OF =
        mgmt.makeEdgeLabel("CONTAINER_OF").multiplicity(Multiplicity.ONE2MANY).make();
    mgmt.addConnection(CONTAINER_OF, Forum, Post);
    EdgeLabel HAS_CREATOR =
        mgmt.makeEdgeLabel("HAS_CREATOR").multiplicity(Multiplicity.MANY2ONE).make();
    mgmt.addConnection(HAS_CREATOR, Post, Person);
    mgmt.addConnection(HAS_CREATOR, Comment, Person);
    EdgeLabel HAS_INTEREST =
        mgmt.makeEdgeLabel("HAS_INTEREST").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(HAS_INTEREST, Person, Tag);
    EdgeLabel HAS_MEMBER =
        mgmt.makeEdgeLabel("HAS_MEMBER").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(HAS_MEMBER, Forum, Person);
    EdgeLabel HAS_MODERATOR =
        mgmt.makeEdgeLabel("HAS_MODERATOR").multiplicity(Multiplicity.MANY2ONE).make();
    mgmt.addConnection(HAS_MODERATOR, Forum, Person);
    EdgeLabel HAS_TAG = mgmt.makeEdgeLabel("HAS_TAG").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(HAS_TAG, Comment, Tag);
    mgmt.addConnection(HAS_TAG, Post, Tag);
    mgmt.addConnection(HAS_TAG, Forum, Tag);
    EdgeLabel HAS_TYPE = mgmt.makeEdgeLabel("HAS_TYPE").multiplicity(Multiplicity.MANY2ONE).make();
    mgmt.addConnection(HAS_TYPE, Tag, TagClass);
    EdgeLabel IS_LOCATED_IN =
        mgmt.makeEdgeLabel("IS_LOCATED_IN").multiplicity(Multiplicity.MANY2ONE).make();
    mgmt.addConnection(IS_LOCATED_IN, Comment, Country);
    mgmt.addConnection(IS_LOCATED_IN, Post, Country);
    mgmt.addConnection(IS_LOCATED_IN, Company, Country);
    mgmt.addConnection(IS_LOCATED_IN, Person, City);
    mgmt.addConnection(IS_LOCATED_IN, University, City);
    EdgeLabel IS_PART_OF =
        mgmt.makeEdgeLabel("IS_PART_OF").multiplicity(Multiplicity.MANY2ONE).make();
    mgmt.addConnection(IS_PART_OF, City, Country);
    mgmt.addConnection(IS_PART_OF, Country, Continent);
    EdgeLabel IS_SUBCLASS_OF =
        mgmt.makeEdgeLabel("IS_SUBCLASS_OF").multiplicity(Multiplicity.MANY2ONE).make();
    mgmt.addConnection(IS_SUBCLASS_OF, TagClass, TagClass);
    EdgeLabel KNOWS = mgmt.makeEdgeLabel("KNOWS").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(KNOWS, Person, Person);
    EdgeLabel LIKES = mgmt.makeEdgeLabel("LIKES").multiplicity(Multiplicity.SIMPLE).make();
    mgmt.addConnection(LIKES, Person, Comment);
    mgmt.addConnection(LIKES, Person, Post);
    // Not modeled: every Comment is a reply of exactly one Comment OR Post (but not both)
    EdgeLabel REPLY_OF = mgmt.makeEdgeLabel("REPLY_OF").multiplicity(Multiplicity.MANY2ONE).make();
    mgmt.addConnection(REPLY_OF, Comment, Comment);
    mgmt.addConnection(REPLY_OF, Comment, Post);
    EdgeLabel STUDY_AT = mgmt.makeEdgeLabel("STUDY_AT").multiplicity(Multiplicity.MULTI).make();
    mgmt.addConnection(STUDY_AT, Person, University);
    EdgeLabel WORK_AT = mgmt.makeEdgeLabel("WORK_AT").multiplicity(Multiplicity.MULTI).make();
    mgmt.addConnection(WORK_AT, Person, Company);

    // Build an index for ID key, because edges/multi-valued properties need this very often
    mgmt.buildIndex("byId", Vertex.class).addKey(idKey).buildCompositeIndex();

    // Edge properties
    mgmt.addProperties(HAS_MEMBER, creationDateKey);
    mgmt.addProperties(KNOWS, creationDateKey);
    mgmt.addProperties(LIKES, creationDateKey);
    mgmt.addProperties(STUDY_AT, classYearKey);
    mgmt.addProperties(WORK_AT, workFromKey);

    // Set file paths for all nodes
    filePathByVertex.put("Comment", "dynamic/comment_0_0.csv");
    filePathByVertex.put("Post", "dynamic/post_0_0.csv");
    filePathByVertex.put("Organisation", "static/organisation_0_0.csv");
    filePathByVertex.put("Place", "static/place_0_0.csv");
    filePathByVertex.put("Forum", "dynamic/forum_0_0.csv");
    filePathByVertex.put("Person", "dynamic/person_0_0.csv");
    filePathByVertex.put("Tag", "static/tag_0_0.csv");
    filePathByVertex.put("TagClass", "static/tagclass_0_0.csv");

    // Set file paths for all edges
    filePathsByEdge.put("CONTAINER_OF", new String[] {"dynamic/forum_containerOf_post_0_0.csv"});
    filePathsByEdge.put("HAS_CREATOR", new String[] {"dynamic/comment_hasCreator_person_0_0.csv",
        "dynamic/post_hasCreator_person_0_0.csv"});
    filePathsByEdge.put("HAS_INTEREST", new String[] {"dynamic/person_hasInterest_tag_0_0.csv"});
    filePathsByEdge.put("HAS_MEMBER", new String[] {"dynamic/forum_hasMember_person_0_0.csv"});
    filePathsByEdge.put("HAS_MODERATOR",
        new String[] {"dynamic/forum_hasModerator_person_0_0.csv"});
    filePathsByEdge.put("HAS_TAG", new String[] {"dynamic/comment_hasTag_tag_0_0.csv",
        "dynamic/forum_hasTag_tag_0_0.csv", "dynamic/post_hasTag_tag_0_0.csv"});
    filePathsByEdge.put("HAS_TYPE", new String[] {"static/tag_hasType_tagclass_0_0.csv"});
    filePathsByEdge.put("IS_LOCATED_IN",
        new String[] {"static/organisation_isLocatedIn_place_0_0.csv",
            "dynamic/comment_isLocatedIn_place_0_0.csv", "dynamic/person_isLocatedIn_place_0_0.csv",
            "dynamic/post_isLocatedIn_place_0_0.csv"});
    filePathsByEdge.put("IS_PART_OF", new String[] {"static/place_isPartOf_place_0_0.csv"});
    filePathsByEdge.put("IS_SUBCLASS_OF",
        new String[] {"static/tagclass_isSubclassOf_tagclass_0_0.csv"});
    filePathsByEdge.put("KNOWS", new String[] {"dynamic/person_knows_person_0_0.csv"});
    filePathsByEdge.put("LIKES",
        new String[] {"dynamic/person_likes_comment_0_0.csv", "dynamic/person_likes_post_0_0.csv"});
    filePathsByEdge.put("REPLY_OF", new String[] {"dynamic/comment_replyOf_comment_0_0.csv",
        "dynamic/comment_replyOf_post_0_0.csv"});
    filePathsByEdge.put("STUDY_AT", new String[] {"dynamic/person_studyAt_organisation_0_0.csv"});
    filePathsByEdge.put("WORK_AT", new String[] {"dynamic/person_workAt_organisation_0_0.csv"});

    // Set file paths for all multi-valued properties
    filePathByProperty.put("speaks", "dynamic/person_speaks_language_0_0.csv");
    filePathByProperty.put("email", "dynamic/person_email_emailaddress_0_0.csv");

    mgmt.commit();
  }

  boolean validateBoolean() {
    GraphTraversalSource g = graph.traversal();

    // Check for missing mandatory properties on vertices
    boolean hasViolatingNodes = g.V().or(
        // All vertices have an id property
        hasNot("id"),
        // Forum
        hasLabel("Forum").or(hasNot("title"), hasNot("creationDate")),
        // Message
        hasLabel(P.within("Comment", "Post")).or(hasNot("browserUsed"), hasNot("creationDate"),
            hasNot("locationIP"), hasNot("length")),
        // Organization/Place/Tag/TagClass
        hasLabel(
            P.within("Company", "University", "City", "Country", "Continent", "Tag", "TagClass"))
                .or(hasNot("name"), hasNot("url")),
        // Person
        hasLabel("Person").or(hasNot("firstName"), hasNot("lastName"), hasNot("gender"),
            hasNot("birthday"), hasNot("email"), hasNot("speaks"), hasNot("browserUsed"),
            hasNot("locationIP"), hasNot("creationDate")))
        .hasNext();

    if (hasViolatingNodes) {
      return false;
    }

    // Check for missing mandatory properties on edges
    boolean hasViolatingEdges = g.E().or(
        // HAS_MEMBER/KNOWS/LIKES
        hasLabel(P.within("HAS_MEMBER", "KNOWS", "LIKES")).hasNot("creationDate"),
        // STUDY_AT
        hasLabel("STUDY_AT").hasNot("classYear"),
        // WORK_AT
        hasLabel("WORK_AT").hasNot("workFrom")).hasNext();

    if (hasViolatingEdges) {
      return false;
    }

    return true;
  }

  Set<Element> validate() {
    throw new NotImplementedException();
  }

  public void loadData(File dataDir) throws Exception {
    // Ensure all data files exist
    Set<String> filePaths = new HashSet<String>();
    filePaths.addAll(filePathByVertex.values());
    for (String[] paths : filePathsByEdge.values()) {
      filePaths.addAll(Arrays.asList(paths));
    }
    filePaths.addAll(filePathByProperty.values());
    for (String filePath : filePaths) {
      if (!new File(dataDir, filePath).isFile()) {
        throw new FileNotFoundException(String.format("Missing data file: %s", filePath));
      }
    }

    JanusGraphTransaction tx = graph.newTransaction();

    CSVFormat vertexCSVFormat = CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true)
        .setDelimiter('|').setNullString("").build();
    CSVFormat edgeCSVFormat = CSVFormat.Builder.create().setSkipHeaderRecord(false)
        .setDelimiter('|').setNullString("").build();

    // Create vertices
    for (String genericVertexName : filePathByVertex.keySet()) {
      System.out.print(String.format("%s ... ", genericVertexName));

      Iterable<CSVRecord> records = vertexCSVFormat
          .parse(new FileReader(new File(dataDir, filePathByVertex.get(genericVertexName))));

      // Iterate over all data records
      int numRecordsLoaded = 0;
      for (CSVRecord record : records) {
        // Get the vertex label, which may be dependent on a value in the record
        VertexLabel vertexLabel;
        if (genericVertexName.equals("Organisation") || genericVertexName.equals("Place")) {
          vertexLabel = tx.getVertexLabel(Util.capitalize(record.get("type")));
        } else {
          vertexLabel = tx.getVertexLabel(genericVertexName);
        }

        JanusGraphVertex vertex = tx.addVertex(vertexLabel.name());

        // Loop over all properties that the vertex is allowed to have
        for (PropertyKey propKey : vertexLabel.mappedProperties()) {
          // Skip the multi-valued properties, we will add them later
          if (!(propKey.name().equals("speaks") || propKey.name().equals("email"))) {
            String rawValue = record.get(propKey.name());
            vertex.property(propKey.name(), parsePropertyValue(propKey, rawValue));
          }
        }

        if (numRecordsLoaded % COMMIT_EVERY_N_RECORDS == 0) {
          tx.commit();
          tx.close();
          tx = graph.newTransaction();
        }
        if (++numRecordsLoaded >= MAX_RECORDS_PER_FILE) {
          break;
        }
      }

      System.out.println("✅");
    }

    tx.commit();
    tx.close();
    tx = graph.newTransaction();
    GraphTraversalSource g = tx.traversal();

    // Set multi-valued properties because they are in separate files
    for (String propName : filePathByProperty.keySet()) {
      System.out.print(String.format("Person.%s ... ", propName));

      Iterable<CSVRecord> records = vertexCSVFormat
          .parse(new FileReader(new File(dataDir, filePathByProperty.get(propName))));

      // Iterate over all data records
      int numRecordsLoaded = 0;
      for (CSVRecord record : records) {
        // Find the vertex for which to set the property
        // Multi-valued properites only exist on the Person vertices, so we can hardcode this
        String vertexId = record.get("Person.id");
        Traversal<Vertex, Vertex> traversal = g.V().has("Person", "id", vertexId);
        if (!traversal.hasNext()) {
          throw new NoSuchElementException(
              String.format("Could not find Person with id %s", vertexId));
          // Silently skip if vertex can't be found
          // continue;
        }
        Vertex vertex = traversal.next();

        if (propName.equals("email")) {
          vertex.property(propName, record.get("email"));
        } else {
          vertex.property(propName, record.get("language"));
        }

        if (numRecordsLoaded % COMMIT_EVERY_N_RECORDS == 0) {
          g.close();
          tx.commit();
          tx.close();
          tx = graph.newTransaction();
          g = tx.traversal();
        }
        if (++numRecordsLoaded >= MAX_RECORDS_PER_FILE) {
          break;
        }
      }

      System.out.println("✅");
    }

    tx.commit();
    tx.close();
    tx = graph.newTransaction();
    g = tx.traversal();

    // Create edges
    for (String edgeLabelName : filePathsByEdge.keySet()) {
      System.out.print(String.format("%s ... ", edgeLabelName));

      String[] files = filePathsByEdge.get(edgeLabelName);
      EdgeLabel edgeLabel = tx.getEdgeLabel(edgeLabelName);

      for (String file : files) {
        Iterable<CSVRecord> records = edgeCSVFormat.parse(new FileReader(new File(dataDir, file)));

        // Read the header record to get the source and target label
        Iterator<CSVRecord> recordIt = records.iterator();
        Iterator<String> headerRecordIt = recordIt.next().iterator();
        String sourceLabel = headerRecordIt.next().split("\\.")[0];
        String targetLabel = headerRecordIt.next().split("\\.")[0];

        // Iterate over all data records
        int numRecordsLoaded = 0;
        while (recordIt.hasNext()) {
          CSVRecord record = recordIt.next();

          String sourceId = record.get(0);
          String targetId = record.get(1);

          Vertex source, target;
          try {
            if (sourceLabel.equals("Organisation")) {
              source = g.V().hasLabel(P.within("Company", "University")).has("id", sourceId).next();
            } else if (sourceLabel.equals("Place")) {
              source = g.V().hasLabel(P.within("City", "Country", "Continent")).has("id", sourceId)
                  .next();
            } else {
              source = g.V().hasLabel(sourceLabel).has("id", sourceId).next();
            }
          } catch (NoSuchElementException e) {
            throw new NoSuchElementException(
                String.format("Could not find %s with id %s", sourceLabel, sourceId));
            // Silently skip edges if source is missing
            // continue;
          }
          try {
            if (targetLabel.equals("Organisation")) {
              target = g.V().hasLabel(P.within("Company", "University")).has("id", targetId).next();
            } else if (targetLabel.equals("Place")) {
              target = g.V().hasLabel(P.within("City", "Country", "Continent")).has("id", targetId)
                  .next();
            } else {
              target = g.V().hasLabel(targetLabel).has("id", targetId).next();
            }
          } catch (NoSuchElementException e) {
            throw new NoSuchElementException(
                String.format("Could not find %s with id %s", targetLabel, targetId));
            // Silently skip edges if target is missing
            // continue;
          }

          Edge edge = source.addEdge(edgeLabelName, target);

          // HACK: assume edge has at most one property, and it is always in column 2
          Iterator<PropertyKey> propIt = edgeLabel.mappedProperties().iterator();
          if (propIt.hasNext()) {
            PropertyKey propKey = propIt.next();
            String rawValue = record.get(2);
            edge.property(propKey.name(), parsePropertyValue(propKey, rawValue));
          }

          if (numRecordsLoaded % COMMIT_EVERY_N_RECORDS == 0) {
            g.close();
            tx.commit();
            tx.close();
            tx = graph.newTransaction();
            g = tx.traversal();
          }
          if (++numRecordsLoaded >= MAX_RECORDS_PER_FILE) {
            break;
          }
        }
      }

      System.out.println("✅");
    }

    tx.commit();
    tx.close();
  }
}
