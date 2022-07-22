// This set of queries can be used to check if the graph conforms to the
// recommendations schema.
// Each query returns the graph objects that violate one of the rules.
// It does not check for things that can be avoided using Neo4j's built-in
// constraints, such as missing mandatory properties.

// Find all nodes that have a label that is not allowed
MATCH (n) WHERE NOT all(label IN labels(n)
WHERE label IN ["Message", "Comment", "Post", "Organisation", "Company", "University", "Place", "City", "Country", "Continent", "Forum", "Person", "Tag", "TagClass"])
RETURN n;
// Find all edges that have a label that is not allowed
MATCH ()-[e]->()
WHERE NOT type(e) IN ["CONTAINER_OF", "HAS_CREATOR", "HAS_INTEREST", "HAS_MEMBER", "HAS_MODERATOR", "HAS_TAG", "HAS_TYPE", "IS_LOCATED_IN", "IS_PART_OF", "IS_SUBCLASS_OF", "KNOWS", "LIKES", "REPLY_OF", "STUDY_AT", "WORK_AT"]
RETURN e;

// Find all nodes that have a property that is not allowed
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
MATCH (n)
WHERE "Comment" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Comment)
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
RETURN n;

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
MATCH ()-[e]->()
WHERE type(e) = "CONTAINER_OF" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.ACTED_IN)
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
RETURN e;

// Find all nodes that have a property with the wrong datatype
// This assumes that if two properties have the same key, they have the same datatype,
// i.e. two node types can't have the same property key with different datatype
WITH {
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
} AS nodePropertyKeys
MATCH (n)
WHERE NOT all(propertyKey IN keys(n) WHERE apoc.meta.type(n[propertyKey]) = nodePropertyKeys[propertyKey])
RETURN n;

// Find all edges that have a property with the wrong datatype
// This has the same limitations as above
WITH {
    creationDate: "ZonedDateTime",
    classYear: "INTEGER",
    workFrom: "INTEGER"
} AS edgePropertyKeys
MATCH ()-[e]->()
WHERE NOT all(propertyKey IN keys(e) WHERE apoc.meta.type(e[propertyKey]) = edgePropertyKeys[propertyKey])
RETURN e;

// Check for edges between wrong types of nodes
MATCH (n)-[e:CONTAINER_OF]->(m) WHERE NOT "Forum" IN labels(n) OR NOT "Post" IN labels(m) RETURN e;
MATCH (n)-[e:HAS_CREATOR]->(m) WHERE NOT "Message" IN labels(n) OR NOT "Person" IN labels(m) RETURN e;
MATCH (n)-[e:HAS_INTEREST]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Tag" IN labels(m) RETURN e;
MATCH (n)-[e:HAS_MEMBER]->(m) WHERE NOT "Forum" IN labels(n) OR NOT "Person" IN labels(m) RETURN e;
MATCH (n)-[e:HAS_MODERATOR]->(m) WHERE NOT "Forum" IN labels(n) OR NOT "Person" IN labels(m) RETURN e;
MATCH (n)-[e:HAS_TAG]->(m) WHERE NOT ("Message" IN labels(n) OR "Forum" IN labels(n)) OR NOT "Tag" IN labels(m) RETURN e;
MATCH (n)-[e:HAS_TYPE]->(m) WHERE NOT "Tag" IN labels(n) OR NOT "TagClass" IN labels(m) RETURN e;
MATCH (n)-[e:IS_LOCATED_IN]->(m) WHERE NOT ("Message" IN labels(n) AND "Country" IN labels(m) OR "Company" IN labels(n) AND "Country" IN labels(m) OR "Person" IN labels(n) AND "City" IN labels(m) OR "University" IN labels(n) AND "City" IN labels(m)) RETURN e;
MATCH (n)-[e:IS_PART_OF]->(m) WHERE NOT (("City" IN labels(n) AND "Country" IN labels(m)) OR ("Country" IN labels(n) AND "Continent" IN labels(m))) RETURN e;
MATCH (n)-[e:IS_SUBCLASS_OF]->(m) WHERE NOT "TagClass" IN labels(n) OR NOT "TagClass" IN labels(m) RETURN e;
MATCH (n)-[e:KNOWS]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Person" IN labels(m) RETURN e;
MATCH (n)-[e:LIKES]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Message" IN labels(m) RETURN e;
MATCH (n)-[e:REPLY_OF]->(m) WHERE NOT ("Comment" IN labels(n) AND "Comment" IN labels(m) OR "Post" IN labels(m)) RETURN e;
MATCH (n)-[e:STUDY_AT]->(m) WHERE NOT "Person" IN labels(n) OR NOT "University" IN labels(m) RETURN e;
MATCH (n)-[e:WORK_AT]->(m) WHERE NOT "Person" IN labels(n) OR NOT "Company" IN labels(m) RETURN e;

// Check for missing mandatory edges
MATCH (post:Post) WHERE NOT ()-[:CONTAINER_OF]->(post) RETURN post;
MATCH (message:Message) WHERE NOT (message)-[:HAS_CREATOR]->() RETURN message;
MATCH (person:Person) WHERE NOT (person)-[:HAS_INTEREST]->() RETURN person;
// MATCH (forum:Forum) WHERE NOT (forum)-[:HAS_MEMBER]->() RETURN forum; // this constraint seems to be violated in data
MATCH (forum:Forum) WHERE NOT (forum)-[:HAS_TAG]->() RETURN forum;
MATCH (tag:Tag) WHERE NOT (tag)-[:HAS_TYPE]->() RETURN tag;
MATCH (organisation:Organisation) WHERE NOT (organisation)-[:IS_LOCATED_IN]->() RETURN organisation;
MATCH (message:Message) WHERE NOT (message)-[:IS_LOCATED_IN]->() RETURN message;
MATCH (person:Person) WHERE NOT (person)-[:IS_LOCATED_IN]->() RETURN person;
MATCH (city:City) WHERE NOT (city)-[:IS_PART_OF]->() RETURN city;
MATCH (country:Country) WHERE NOT (country)-[:IS_PART_OF]->() RETURN country;
MATCH (country:Country) WHERE NOT ()-[:IS_PART_OF]->(country) RETURN country;
MATCH (continent:Continent) WHERE NOT ()-[:IS_PART_OF]->(continent) RETURN continent;
