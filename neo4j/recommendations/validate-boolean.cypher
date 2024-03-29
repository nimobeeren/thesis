// This set of queries can be used to check if the graph conforms to the
// recommendations schema.
// These queries don't return the objects that violate the rules, instead they
// return `true` if the graph conforms.
// It does not check for things that can be avoided using Neo4j's built-in
// constraints, such as missing mandatory properties.

// Check if there are any nodes that have a label set that is not allowed
// NOTE: this depends on the order of labels in the list, but it seems to be consistent
WITH
[
    ["Movie"], ["Genre"], ["User"], ["Actor", "Person"], ["Director", "Person"],
    ["Actor", "Director", "Person"]
] AS allowedLabelSets
MATCH (n)
WHERE NOT labels(n) IN allowedLabelSets
RETURN count(n) = 0;

// Check if there are any edge labels that are not allowed
CALL db.relationshipTypes() YIELD relationshipType AS allTypes
RETURN all(type IN collect(allTypes) WHERE type IN ["IN_GENRE", "RATED", "ACTED_IN", "DIRECTED"]);

WITH {
    Movie: ["budget", "countries", "imdbId", "imdbRating", "imdbVotes", "languages", "movieId", "plot", "poster", "released", "revenue", "runtime", "title", "tmdbId", "url", "year"],
    Person: ["bio", "born", "bornIn", "died", "imdbId", "name", "poster", "tmdbId", "url"],
    User: ["userId", "name"],
    Genre: ["name"]
} AS allowedNodeProperties
CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, propertyTypes
// Find node properties that are not allowed
// Note that this doesn't work if a node has more than one of these labels
WHERE "Movie" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Movie
OR "Person" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Person
OR "User" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.User
OR "Genre" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Genre
// Find node properties with the wrong datatype
OR "Movie" IN nodeLabels AND (
    propertyName = "budget" AND propertyTypes <> ["Long"]
    OR propertyName = "countries" AND propertyTypes <> ["StringArray"]
    OR propertyName = "imdbId" AND propertyTypes <> ["String"]
    OR propertyName = "imdbRating" AND propertyTypes <> ["Double"]
    OR propertyName = "imdbVotes" AND propertyTypes <> ["Long"]
    OR propertyName = "languages" AND propertyTypes <> ["StringArray"]
    OR propertyName = "movieId" AND propertyTypes <> ["String"]
    OR propertyName = "plot" AND propertyTypes <> ["String"]
    OR propertyName = "poster" AND propertyTypes <> ["String"]
    OR propertyName = "released" AND propertyTypes <> ["String"]
    OR propertyName = "revenue" AND propertyTypes <> ["Long"]
    OR propertyName = "runtime" AND propertyTypes <> ["Long"]
    OR propertyName = "title" AND propertyTypes <> ["String"]
    OR propertyName = "tmdbId" AND propertyTypes <> ["String"]
    OR propertyName = "url" AND propertyTypes <> ["String"]
    OR propertyName = "year" AND propertyTypes <> ["Long"]
)
OR "Person" IN nodeLabels AND (
    propertyName = "bio" AND propertyTypes <> ["String"]
    OR propertyName = "born" AND propertyTypes <> ["Date"]
    OR propertyName = "bornIn" AND propertyTypes <> ["String"]
    OR propertyName = "died" AND propertyTypes <> ["Date"]
    OR propertyName = "imdbId" AND propertyTypes <> ["String"]
    OR propertyName = "name" AND propertyTypes <> ["String"]
    OR propertyName = "poster" AND propertyTypes <> ["String"]
    OR propertyName = "tmdbId" AND propertyTypes <> ["String"]
    OR propertyName = "url" AND propertyTypes <> ["String"]
)
OR "User" IN nodeLabels AND (
    propertyName = "userId" AND propertyTypes <> ["String"]
    OR propertyName = "name" AND propertyTypes <> ["String"]
)
OR "Genre" IN nodeLabels AND (
    propertyName = "name" AND propertyTypes <> ["String"]
)
RETURN count(propertyName) = 0;

// Find edge properties that are not allowed
WITH {
    ACTED_IN: ["role"],
    DIRECTED: ["role"],
    RATED: ["rating", "timestamp"],
    IN_GENRE: [null]
} AS allowedEdgeProperties
CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes
WHERE relType = ":`ACTED_IN`" AND NOT propertyName IN allowedEdgeProperties.ACTED_IN
OR relType = ":`DIRECTED`" AND NOT propertyName IN allowedEdgeProperties.DIRECTED
OR relType = ":`RATED`" AND NOT propertyName IN allowedEdgeProperties.RATED
OR relType = ":`IN_GENRE`" AND NOT propertyName IN allowedEdgeProperties.IN_GENRE
// Find edge properties with the wrong datatype
OR relType = ":`ACTED_IN`" AND propertyName = "role" AND propertyTypes <> ["String"]
OR relType = ":`DIRECTED`" AND propertyName = "role" AND propertyTypes <> ["String"]
OR relType = ":`RATED`" AND (
    propertyName = "rating" AND propertyTypes <> ["Double"]
    OR propertyName = "timestamp" AND propertyTypes <> ["Long"]
)
RETURN count(propertyName) = 0;

// Check for edges between wrong types of nodes
// We can't do this with the schema statistics, because this finds that some Actors have DIRECTED eges,
// which is allowed, but only when they are also Directors
MATCH (n)-[e]->(m)
WHERE type(e) = "ACTED_IN" AND (NOT "Actor" IN labels(n) OR NOT labels(m) = ["Movie"])
OR type(e) = "DIRECTED" AND (NOT "Director" IN labels(n) OR NOT labels(m) = ["Movie"])
OR type(e) = "IN_GENRE" AND (NOT labels(n) = ["Movie"] OR NOT labels(m) = ["Genre"])
OR type(e) = "RATED" AND (NOT labels(n) = ["User"] OR NOT labels(m) = ["Movie"])
RETURN count(e) = 0;

// Check for missing mandatory edges
MATCH (n)
WHERE "Actor" IN labels(n) AND NOT (n)-[:ACTED_IN]->(:Movie)
OR "Director" IN labels(n) AND NOT (n)-[:DIRECTED]->(:Movie)
OR "Movie" IN labels(n) AND NOT (n)-[:IN_GENRE]->(:Genre)
RETURN count(n) = 0;
