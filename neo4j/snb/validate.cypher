// This set of queries can be used to check if the graph conforms to the
// recommendations schema.
// Each query returns the graph objects that violate one of the rules.
// It does not check for things that can be avoided using Neo4j's built-in
// constraints, such as missing mandatory properties.

// Find all violating nodes
WITH
[
    ["Comment", "Message"], ["Company", "Organisation"], ["Organisation", "University"], ["Message", "Post"],
    ["TagClass"], ["Tag"], ["Person"], ["Place", "City"], ["Country", "Place"], ["Place", "Continent"], ["Forum"]
] AS allowedNodeLabelSets,
{
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
} AS allowedNodeProperties,
{
    id: "INTEGER",
    creationDate: "ZonedDateTime",
    locationIP: "STRING",
    browserUsed: "STRING",
    content: "STRING",
    length: "INTEGER",
    imageFile: "STRING",
    language: "STRING",
    name: "STRING",
    url: "STRING",
    title: "STRING",
    firstName: "STRING",
    lastName: "STRING",
    gender: "STRING",
    birthday: "LocalDate",
    speaks: "String[]",
    email: "String[]"
} AS nodePropertyTypes
MATCH (n)
// Find all nodes that have a label set that is not allowed
// NOTE: this depends on the order of labels in the list, but it seems to be consistent
WHERE NOT labels(n) IN allowedNodeLabelSets
// Find all nodes that have a property that is not allowed
// Note that this doesn't work if a node has more than one of these labels
OR "Comment" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Comment)
OR "Post" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Post)
OR "Company" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Company)
OR "University" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.University)
OR "City" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.City)
OR "Country" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Country)
OR "Continent" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Continent)
OR "Forum" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Forum)
OR "Person" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Person)
OR "Tag" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Tag)
OR "TagClass" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.TagClass)
// Find all nodes that have a property with the wrong datatype
// This assumes that if two properties have the same key, they have the same datatype,
// i.e. two node types can't have the same property key with different datatype
OR NOT all(propertyKey IN keys(n) WHERE apoc.meta.type(n[propertyKey]) = nodePropertyTypes[propertyKey])
// Check for missing mandatory edges
OR "Post" IN labels(n) AND NOT ()-[:CONTAINER_OF]->(n)
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
RETURN n;

// Find all violating edges
WITH
["CONTAINER_OF", "HAS_CREATOR", "HAS_INTEREST", "HAS_MEMBER", "HAS_MODERATOR", "HAS_TAG", "HAS_TYPE", "IS_LOCATED_IN",
"IS_PART_OF", "IS_SUBCLASS_OF", "KNOWS", "LIKES", "REPLY_OF", "STUDY_AT", "WORK_AT"] AS allowedEdgeLabels,
{
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
} AS allowedEdgeProperties,
{
    creationDate: "ZonedDateTime",
    classYear: "INTEGER",
    workFrom: "INTEGER"
} AS edgePropertyTypes
MATCH (n)-[e]->(m)
// Find all edges that have a label that is not allowed
WHERE NOT type(e) IN allowedEdgeLabels
// Find edge properties that are not allowed
OR type(e) = "CONTAINER_OF" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.ACTED_IN)
OR type(e) = "HAS_CREATOR" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.HAS_CREATOR)
OR type(e) = "HAS_INTEREST" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.HAS_INTEREST)
OR type(e) = "HAS_MEMBER" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.HAS_MEMBER)
OR type(e) = "HAS_MODERATOR" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.HAS_MODERATOR)
OR type(e) = "HAS_TAG" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.HAS_TAG)
OR type(e) = "HAS_TYPE" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.HAS_TYPE)
OR type(e) = "IS_LOCATED_IN" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.IS_LOCATED_IN)
OR type(e) = "IS_PART_OF" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.IS_PART_OF)
OR type(e) = "IS_SUBCLASS_OF" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.IS_SUBCLASS_OF)
OR type(e) = "KNOWS" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.KNOWS)
OR type(e) = "LIKES" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.LIKES)
OR type(e) = "REPLY_OF" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.REPLY_OF)
OR type(e) = "STUDY_AT" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.STUDY_AT)
OR type(e) = "WORK_AT" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.WORK_AT)
// Find all edges that have a property with the wrong datatype
// This has the same limitations as for nodes
OR NOT all(propertyKey IN keys(e) WHERE apoc.meta.type(e[propertyKey]) = edgePropertyTypes[propertyKey])
// Check for edges between wrong types of nodes
OR type(e) = "CONTAINER_OF" AND (NOT "Forum" IN labels(n) OR NOT "Post" IN labels(m))
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
RETURN e;
