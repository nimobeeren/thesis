package com.github.nimobeeren.thesis.janusgraph;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Element;
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

  Map<String, String> filePathByVertex = new HashMap<String, String>();
  Map<String, String> filePathByEdge = new HashMap<String, String>();
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  CSVFormat csvFormat;

  SNBModel(JanusGraph graph) {
    super(graph);
    this.csvFormat = CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true)
        .setDelimiter('|').setNullString("").build();
  }

  Iterable<CSVRecord> parseFile(File dir, String fileName) throws IOException {
    return csvFormat.parse(new FileReader(new File(dir, fileName)));
  }

  Object parsePropertyValue(PropertyKey propKey, String rawValue) throws ParseException {
    if (rawValue == null) {
      return rawValue;
    }

    if (propKey.dataType() == Date.class) {
      return dateFormat.parse(rawValue);
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
    PropertyKey locationIpKey = mgmt.makePropertyKey("locationIp").dataType(String.class).make();
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
    mgmt.addProperties(Comment, idKey, browserUsedKey, locationIpKey, contentKey, lengthKey);
    mgmt.addProperties(Post, idKey, browserUsedKey, locationIpKey, contentKey, lengthKey,
        languageKey, imageFileKey); // Not modeled: language and imageFile are mutually exclusive
    // Organization
    mgmt.addProperties(Company, idKey, nameKey, urlKey);
    mgmt.addProperties(University, idKey, nameKey, urlKey);
    // Place
    mgmt.addProperties(City, idKey, nameKey, urlKey);
    mgmt.addProperties(Country, idKey, nameKey, urlKey);
    mgmt.addProperties(Continent, idKey, nameKey, urlKey);
    // Others
    mgmt.addProperties(Forum, idKey, creationDateKey, titleKey);
    mgmt.addProperties(Person, idKey, creationDateKey, firstNameKey, lastNameKey, genderKey,
        birthdayKey, emailKey, speaksKey, browserUsedKey, locationIpKey);
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

    // Edge properties
    mgmt.addProperties(HAS_MEMBER, creationDateKey);
    mgmt.addProperties(KNOWS, creationDateKey);
    mgmt.addProperties(LIKES, creationDateKey);
    mgmt.addProperties(STUDY_AT, classYearKey);
    mgmt.addProperties(WORK_AT, workFromKey);

    // Set file path for all schema elements
    // filePathByVertex.put("Comment", "dynamic/comment_0_0.csv");
    // filePathByVertex.put("Post", "dynamic/post_0_0.csv");
    // filePathByVertex.put("Organisation", "static/organisation_0_0.csv");
    // filePathByVertex.put("Place", "static/place_0_0.csv");
    // filePathByVertex.put("Forum", "dynamic/forum_0_0.csv");
    // filePathByVertex.put("Person", "dynamic/person_0_0.csv");
    filePathByVertex.put("Tag", "static/tag_0_0.csv");
    // filePathByVertex.put("TagClass", "static/tagclass_0_0.csv");
    filePathByEdge.put("CONTAINER_OF", "dynamic/forum_containerOf_post_0_0.csv");
    filePathByEdge.put("HAS_CREATOR", "dynamic/comment_hasCreator_person_0_0.csv");
    filePathByEdge.put("HAS_CREATOR", "dynamic/post_hasCreator_person_0_0.csv");
    filePathByEdge.put("HAS_INTEREST", "dynamic/person_hasInterest_tag_0_0.csv");
    filePathByEdge.put("HAS_MEMBER", "dynamic/forum_hasMember_person_0_0.csv");
    filePathByEdge.put("HAS_MODERATOR", "dynamic/forum_hasModerator_person_0_0.csv");
    filePathByEdge.put("HAS_TAG", "static/comment_hasTag_tag_0_0.csv");
    filePathByEdge.put("HAS_TAG", "dynamic/forum_hasTag_tag_0_0.csv");
    filePathByEdge.put("HAS_TAG", "dynamic/post_hasTag_tag_0_0.csv");
    filePathByEdge.put("HAS_TYPE", "static/tag_hasType_tagclass_0_0.csv");
    filePathByEdge.put("IS_LOCATED_IN", "static/organisation_isLocatedIn_place_0_0.csv");
    filePathByEdge.put("IS_LOCATED_IN", "dynamic/comment_isLocatedIn_place_0_0.csv");
    filePathByEdge.put("IS_LOCATED_IN", "dynamic/person_isLocatedIn_place_0_0.csv");
    filePathByEdge.put("IS_LOCATED_IN", "dynamic/post_isLocatedIn_place_0_0.csv");
    filePathByEdge.put("IS_PART_OF", "static/place_isPartOf_place_0_0.csv");
    filePathByEdge.put("IS_SUBCLASS_OF", "static/tagclass_isSubclassOf_tagclass_0_0.csv");
    filePathByEdge.put("KNOWS", "dynamic/person_knows_person_0_0.csv");
    filePathByEdge.put("LIKES", "dynamic/person_likes_comment_0_0.csv");
    filePathByEdge.put("LIKES", "dynamic/person_likes_post_0_0.csv");
    filePathByEdge.put("REPLY_OF", "dynamic/comment_replyOf_comment_0_0.csv.csv");
    filePathByEdge.put("REPLY_OF", "dynamic/comment_replyOf_post_0_0.csv");
    filePathByEdge.put("STUDY_AT", "dynamic/person_studyAt_organisation_0_0.csv");
    filePathByEdge.put("WORK_AT", "dynamic/person_workAt_organisation_0_0.csv");

    mgmt.commit();
  }

  boolean validateBoolean() {
    GraphTraversalSource g = graph.traversal();

    // Check for missing mandatory properties on vertices
    return g.V().hasLabel("Tag").or(__.hasNot("id"), __.hasNot("name"), __.hasNot("url")).hasNext();
  }

  Set<Element> validate() {
    throw new NotImplementedException();
  }

  public void loadData(File dataDir) throws IOException, ParseException {
    JanusGraphTransaction tx = graph.buildTransaction().enableBatchLoading().start();

    for (String genericVertexName : filePathByVertex.keySet()) {
      Iterable<CSVRecord> records = parseFile(dataDir, filePathByVertex.get(genericVertexName));
      for (CSVRecord record : records) {
        // Get the vertex label, which may be dependent on a value in the record
        VertexLabel vertexLabel;
        if (genericVertexName == "Organisation" || genericVertexName == "Place") {
          vertexLabel = tx.getVertexLabel(Util.capitalize(record.get("type")));
        } else {
          vertexLabel = tx.getVertexLabel(genericVertexName);
        }
        JanusGraphVertex vertex = tx.addVertex(vertexLabel.name());
        // Loop over all properties that the vertex is allowed to have
        for (PropertyKey propKey : vertexLabel.mappedProperties()) {
          String rawValue = record.get(propKey.name());
          vertex.property(propKey.name(), parsePropertyValue(propKey, rawValue));
        }
      }
    }

    // TODO: deal with set properties being separate files (email/speaks)

    // TODO: deal with same edge label coming from multiple files

    tx.commit();
  }
}
