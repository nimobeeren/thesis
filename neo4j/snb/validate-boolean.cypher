// This set of queries can be used to check if the graph conforms to the
// recommendations schema.
// Most queries don't return the objects that violate the rules, instead they
// return an empty result or `true` if the graph conforms.
// It does not check for things that can be avoided using Neo4j's built-in
// constraints, such as missing mandatory properties.

// Find node labels that are not allowed
CALL db.labels() YIELD label AS allLabels
RETURN all(label IN collect(allLabels) WHERE label IN ["Message", "Comment", "Post", "Organisation", "Company", "University", "Place", "City", "Country", "Continent", "Forum", "Person", "Tag", "TagClass"]);
// Find edge labels that are not allowed
CALL db.relationshipTypes() YIELD relationshipType AS allTypes
RETURN all(type IN collect(allTypes) WHERE type IN ["CONTAINER_OF", "HAS_CREATOR", "HAS_INTEREST", "HAS_MEMBER", "HAS_MODERATOR", "HAS_TAG", "HAS_TYPE", "IS_LOCATED_IN", "IS_PART_OF", "IS_SUBCLASS_OF", "KNOWS", "LIKES", "REPLY_OF", "STUDY_AT", "WORK_AT"]);

// Find node properties that are not allowed
// Note that this doesn't work if a node has more than one of these labels
WITH {
    Comment: ["id", "browserUsed", "creationDate", "locationIP", "content", "length"],
    Post: ["id", "browserUsed", "creationDate", "locationIP", "content", "length", "language", "imageFile"],
    Company: ["id", "name", "url"],
    University: ["id", "name", "url"],
    City: ["id", "name", "url"],
    Country: ["id", "name", "url"],
    Continent: ["id", "name", "url"],
    Forum: ["id", "creationDate", "title"],
    Person: ["id", "firstName", "lastName", "gender", "birthday", "email", "speaks", "browserUsed", "locationIP", "creationDate"],
    Tag: ["id", "name", "url"],
    TagClass: ["id", "name", "url"]
} AS allowedNodeProperties
CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName
WHERE "Comment" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Movie
OR "Post" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Post
OR "Company" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Company
OR "University" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.University
OR "City" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.City
OR "Country" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Country
OR "Continent" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Continent
OR "Forum" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Forum
OR "Person" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Person
OR "Tag" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Tag
OR "TagClass" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.TagClass
RETURN nodeLabels, propertyName;

// Find edge properties that are not allowed
WITH {
    CONTAINER_OF: [],
    HAS_CREATOR: [],
    HAS_INTEREST: [],
    HAS_MEMBER: ["creationDate"],
    HAS_MODERATOR: [],
    HAS_TAG: [],
    HAS_TYPE: [],
    IS_LOCATED_IN: [],
    IS_PART_OF: [],
    IS_SUBCLASS_OF: [],
    KNOWS: ["creationDate"],
    LIKES: ["creationDate"],
    REPLY_OF: [],
    STUDY_AT: ["classYear"],
    WORK_AT: ["workFrom"]
} AS allowedEdgeProperties
CALL db.schema.relTypeProperties() YIELD relType, propertyName
OR relType = ":`CONTAINER_OF`" AND NOT propertyName IN allowedEdgeProperties.CONTAINER_OF
OR relType = ":`HAS_CREATOR`" AND NOT propertyName IN allowedEdgeProperties.HAS_CREATOR
OR relType = ":`HAS_INTEREST`" AND NOT propertyName IN allowedEdgeProperties.HAS_INTEREST
OR relType = ":`HAS_MEMBER`" AND NOT propertyName IN allowedEdgeProperties.HAS_MEMBER
OR relType = ":`HAS_MODERATOR`" AND NOT propertyName IN allowedEdgeProperties.HAS_MODERATOR
OR relType = ":`HAS_TAG`" AND NOT propertyName IN allowedEdgeProperties.HAS_TAG
OR relType = ":`HAS_TYPE`" AND NOT propertyName IN allowedEdgeProperties.HAS_TYPE
OR relType = ":`IS_LOCATED_IN`" AND NOT propertyName IN allowedEdgeProperties.IS_LOCATED_IN
OR relType = ":`IS_PART_OF`" AND NOT propertyName IN allowedEdgeProperties.IS_PART_OF
OR relType = ":`IS_SUBCLASS_OF`" AND NOT propertyName IN allowedEdgeProperties.IS_SUBCLASS_OF
OR relType = ":`KNOWS`" AND NOT propertyName IN allowedEdgeProperties.KNOWS
OR relType = ":`LIKES`" AND NOT propertyName IN allowedEdgeProperties.LIKES
OR relType = ":`REPLY_OF`" AND NOT propertyName IN allowedEdgeProperties.REPLY_OF
OR relType = ":`STUDY_AT`" AND NOT propertyName IN allowedEdgeProperties.STUDY_AT
OR relType = ":`WORK_AT`" AND NOT propertyName IN allowedEdgeProperties.WORK_AT
RETURN relType, propertyName;

// Find node properties with the wrong datatype
CALL db.schema.nodeTypeProperties() YIELD propertyName, propertyTypes
WHERE propertyName = "id" AND propertyTypes <> ["Long"]
OR propertyName = "creationDate" AND propertyTypes <> ["DateTime"]
OR propertyName = "locationIP" AND propertyTypes <> ["String"]
OR propertyName = "browserUsed" AND propertyTypes <> ["String"]
OR propertyName = "content" AND propertyTypes <> ["String"]
OR propertyName = "length" AND propertyTypes <> ["Long"]
OR propertyName = "imageFile" AND propertyTypes <> ["String"]
OR propertyName = "language" AND propertyTypes <> ["String"]
OR propertyName = "name" AND propertyTypes <> ["String"]
OR propertyName = "url" AND propertyTypes <> ["String"]
OR propertyName = "title" AND propertyTypes <> ["String"]
OR propertyName = "firstName" AND propertyTypes <> ["String"]
OR propertyName = "lastName" AND propertyTypes <> ["String"]
OR propertyName = "gender" AND propertyTypes <> ["String"]
OR propertyName = "birthday" AND propertyTypes <> ["Date"]
OR propertyName = "speaks" AND propertyTypes <> ["StringArray"]
OR propertyName = "email" AND propertyTypes <> ["StringArray"]
RETURN propertyName, propertyTypes;

// Find edge properties with the wrong datatype
CALL db.schema.relTypeProperties() YIELD propertyName, propertyTypes
WHERE propertyName = "creationDate" AND propertyTypes <> ["DateTime"]
OR propertyName = "classYear" AND propertyTypes <> ["Long"]
OR propertyName = "workFrom" AND propertyTypes <> ["Long"]
RETURN propertyName, propertyTypes;

// Check for edges between wrong types of nodes
// TODO: do this with schema statistics?
MATCH (n)-[e:CONTAINER_OF]->(m) WHERE NOT "Forum" IN labels(n) OR NOT "Post" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:HAS_CREATOR]->(m) WHERE NOT "Message" IN labels(n) OR NOT "Person" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:HAS_INTEREST]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Tag" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:HAS_MEMBER]->(m) WHERE NOT "Forum" IN labels(n) OR NOT "Person" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:HAS_MODERATOR]->(m) WHERE NOT "Forum" IN labels(n) OR NOT "Person" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:HAS_TAG]->(m) WHERE NOT ("Message" IN labels(n) OR "Forum" IN labels(n)) OR NOT "Tag" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:HAS_TYPE]->(m) WHERE NOT "Tag" IN labels(n) OR NOT "TagClass" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:IS_LOCATED_IN]->(m) WHERE NOT ("Message" IN labels(n) AND "Country" IN labels(m) OR "Company" IN labels(n) AND "Country" IN labels(m) OR "Person" IN labels(n) AND "City" IN labels(m) OR "University" IN labels(n) AND "City" IN labels(m)) RETURN count(e) = 0;
MATCH (n)-[e:IS_PART_OF]->(m) WHERE NOT (("City" IN labels(n) AND "Country" IN labels(m)) OR ("Country" IN labels(n) AND "Continent" IN labels(m))) RETURN count(e) = 0;
MATCH (n)-[e:IS_SUBCLASS_OF]->(m) WHERE NOT "TagClass" IN labels(n) OR NOT "TagClass" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:KNOWS]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Person" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:LIKES]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Message" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:REPLY_OF]->(m) WHERE NOT ("Comment" IN labels(n) AND "Comment" IN labels(m) OR "Post" IN labels(m)) RETURN count(e) = 0;
MATCH (n)-[e:STUDY_AT]->(m) WHERE NOT "Person" IN labels(n) OR NOT "University" IN labels(m) RETURN count(e) = 0;
MATCH (n)-[e:WORK_AT]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Company" IN labels(m) RETURN count(e) = 0;

// Check for missing mandatory edges
MATCH (post:Post) WHERE NOT ()-[:CONTAINER_OF]->(post) RETURN count(post) = 0;
MATCH (message:Message) WHERE NOT (message)-[:HAS_CREATOR]->() RETURN count(message) = 0;
MATCH (person:Person) WHERE NOT (person)-[:HAS_INTEREST]->() RETURN count(person) = 0;
// MATCH (forum:Forum) WHERE NOT (forum)-[:HAS_MEMBER]->() RETURN count(forum) = 0; // this constraint seems to be violated in data
MATCH (forum:Forum) WHERE NOT (forum)-[:HAS_TAG]->() RETURN count(forum) = 0;
MATCH (tag:Tag) WHERE NOT (tag)-[:HAS_TYPE]->() RETURN count(tag) = 0;
MATCH (organisation:Organisation) WHERE NOT (organisation)-[:IS_LOCATED_IN]->() count(RETURN) = 0 organisation;
MATCH (message:Message) WHERE NOT (message)-[:IS_LOCATED_IN]->() RETURN count(message) = 0;
MATCH (person:Person) WHERE NOT (person)-[:IS_LOCATED_IN]->() RETURN count(person) = 0;
MATCH (city:City) WHERE NOT (city)-[:IS_PART_OF]->() RETURN count(city) = 0;
MATCH (country:Country) WHERE NOT (country)-[:IS_PART_OF]->() RETURN count(country) = 0;
MATCH (country:Country) WHERE NOT ()-[:IS_PART_OF]->(country) RETURN count(country) = 0;
MATCH (continent:Continent) WHERE NOT ()-[:IS_PART_OF]->(continent) RETURN count(continent) = 0;
