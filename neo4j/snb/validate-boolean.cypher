// This set of queries can be used to check if the graph conforms to the
// SNB schema.
// These queries don't return the objects that violate the rules, instead they
// return `true` if the graph conforms.
// It does not check for things that can be avoided using Neo4j's built-in
// constraints, such as missing mandatory properties.

// Check if there are any nodes that have a label set that is not allowed
// NOTE: this depends on the order of labels in the list, but it seems to be consistent
WITH
[
    ["Comment", "Message"], ["Company", "Organisation"], ["Organisation", "University"], ["Message", "Post"], ["TagClass"],
    ["Tag"], ["Person"], ["Place", "City"], ["Country", "Place"], ["Place", "Continent"], ["Forum"]
] AS allowedNodeLabelSets
MATCH (n)
WHERE NOT labels(n) IN allowedNodeLabelSets
RETURN count(n) = 0;

// Check if there are any edge labels that are not allowed
WITH
[
    "CONTAINER_OF", "HAS_CREATOR", "HAS_INTEREST", "HAS_MEMBER", "HAS_MODERATOR", "HAS_TAG", "HAS_TYPE", "IS_LOCATED_IN",
    "IS_PART_OF", "IS_SUBCLASS_OF", "KNOWS", "LIKES", "REPLY_OF", "STUDY_AT", "WORK_AT"
] AS allowedEdgeLabels
CALL db.relationshipTypes() YIELD relationshipType AS allTypes
RETURN all(type IN collect(allTypes) WHERE type IN allowedEdgeLabels);

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
CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, propertyTypes
// Find node properties that are not allowed
// Note that this doesn't work if a node has more than one of these labels
WHERE "Comment" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Comment
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
// Find node properties with the wrong datatype
OR propertyName = "id" AND propertyTypes <> ["Long"]
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
RETURN count(propertyName) = 0;

WITH {
    CONTAINER_OF: [null],
    HAS_CREATOR: [null],
    HAS_INTEREST: [null],
    HAS_MEMBER: ["creationDate"],
    HAS_MODERATOR: [null],
    HAS_TAG: [null],
    HAS_TYPE: [null],
    IS_LOCATED_IN: [null],
    IS_PART_OF: [null],
    IS_SUBCLASS_OF: [null],
    KNOWS: ["creationDate"],
    LIKES: ["creationDate"],
    REPLY_OF: [null],
    STUDY_AT: ["classYear"],
    WORK_AT: ["workFrom"]
} AS allowedEdgeProperties
CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes
// Find edge properties that are not allowed
WHERE relType = ":`CONTAINER_OF`" AND NOT propertyName IN allowedEdgeProperties.CONTAINER_OF
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
// Find edge properties with the wrong datatype
OR propertyName = "creationDate" AND propertyTypes <> ["DateTime"]
OR propertyName = "classYear" AND propertyTypes <> ["Long"]
OR propertyName = "workFrom" AND propertyTypes <> ["Long"]
RETURN count(propertyName) = 0;

// Check for edges between wrong types of nodes
// TODO: do this with schema statistics? db.schema.visualization() or apoc.meta.schema()
MATCH (n)-[e]->(m)
WHERE type(e) = "CONTAINER_OF" AND (NOT "Forum" IN labels(n) OR NOT "Post" IN labels(m))
OR type(e) = "HAS_CREATOR" AND (NOT "Message" IN labels(n) OR NOT "Person" IN labels(m))
OR type(e) = "HAS_INTEREST" AND (NOT "Person" IN labels(n) OR NOT "Tag" IN labels(m))
OR type(e) = "HAS_MEMBER" AND (NOT "Forum" IN labels(n) OR NOT "Person" IN labels(m))
OR type(e) = "HAS_MODERATOR" AND (NOT "Forum" IN labels(n) OR NOT "Person" IN labels(m))
OR type(e) = "HAS_TAG" AND (NOT ("Message" IN labels(n) OR "Forum" IN labels(n)) OR NOT "Tag" IN labels(m))
OR type(e) = "HAS_TYPE" AND (NOT "Tag" IN labels(n) OR NOT "TagClass" IN labels(m))
OR type(e) = "IS_LOCATED_IN" AND (NOT ("Message" IN labels(n) AND "Country" IN labels(m) OR "Company" IN labels(n) AND "Country" IN labels(m) OR "Person" IN labels(n) AND "City" IN labels(m) OR "University" IN labels(n) AND "City" IN labels(m)))
OR type(e) = "IS_PART_OF" AND (NOT (("City" IN labels(n) AND "Country" IN labels(m)) OR ("Country" IN labels(n) AND "Continent" IN labels(m))))
OR type(e) = "IS_SUBCLASS_OF" AND (NOT "TagClass" IN labels(n) OR NOT "TagClass" IN labels(m))
OR type(e) = "KNOWS" AND (NOT "Person" IN labels(n) OR NOT "Person" IN labels(m))
OR type(e) = "LIKES" AND (NOT "Person" IN labels(n) OR NOT "Message" IN labels(m))
OR type(e) = "REPLY_OF" AND (NOT ("Comment" IN labels(n) AND "Comment" IN labels(m) OR "Post" IN labels(m)))
OR type(e) = "STUDY_AT" AND (NOT "Person" IN labels(n) OR NOT "University" IN labels(m))
OR type(e) = "WORK_AT" AND (NOT "Person" IN labels(n) OR NOT "Company" IN labels(m))
RETURN count(e) = 0;

// Check for missing mandatory edges
MATCH (n)
WHERE "Post" IN labels(n) AND NOT ()-[:CONTAINER_OF]->(n)
OR "Message" IN labels(n) AND NOT (n)-[:HAS_CREATOR]->()
OR "Person" IN labels(n) AND NOT (n)-[:HAS_INTEREST]->()
// OR "Forum" IN labels(n) AND NOT (n)-[:HAS_MEMBER]->() // this constraint seems to be violated in data
OR "Forum" IN labels(n) AND NOT (n)-[:HAS_TAG]->()
OR "Tag" IN labels(n) AND NOT (n)-[:HAS_TYPE]->()
OR "Organisation" IN labels(n) AND NOT (n)-[:IS_LOCATED_IN]->()
OR "Message" IN labels(n) AND NOT (n)-[:IS_LOCATED_IN]->()
OR "Person" IN labels(n) AND NOT (n)-[:IS_LOCATED_IN]->()
OR "City" IN labels(n) AND NOT (n)-[:IS_PART_OF]->()
OR "Country" IN labels(n) AND NOT (n)-[:IS_PART_OF]->()
OR "Country" IN labels(n) AND NOT ()-[:IS_PART_OF]->(n)
OR "Continent" IN labels(n) AND NOT ()-[:IS_PART_OF]->(n)
RETURN count(n) = 0;
