// Find node labels that are not allowed
CALL db.labels() YIELD label AS allLabels
RETURN all(label IN collect(allLabels) WHERE label IN ["Movie", "Genre", "User", "Actor", "Director", "Person"]);
// Find edge labels that are not allowed
CALL db.relationshipTypes() YIELD relationshipType AS allTypes
RETURN all(type IN collect(allTypes) WHERE type IN ["IN_GENRE", "RATED", "ACTED_IN", "DIRECTED"]);

// Find node properties that are not allowed
WITH {
    Movie: ["budget", "countries", "imdbId", "imdbRating", "imdbVotes", "languages", "movieId", "plot", "poster", "released", "revenue", "runtime", "title", "tmdbId", "url", "year"],
    Person: ["bio", "born", "bornIn", "died", "imdbId", "name", "poster", "tmdbId", "url"],
    User: ["userId", "name"],
    Genre: ["name"]
} AS allowedNodeProperties
CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName
WHERE "Movie" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Movie
OR "Person" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Person
OR "User" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.User
OR "Genre" IN nodeLabels AND NOT propertyName IN allowedNodeProperties.Genre
RETURN nodeLabels, propertyName;

// Find edge properties that are not allowed
WITH {
    ACTED_IN: ["role"],
    DIRECTED: ["role"],
    RATED: ["rating", "timestamp"],
    IN_GENRE: [null]
} AS allowedEdgeProperties
CALL db.schema.relTypeProperties() YIELD relType, propertyName
WHERE relType = ":`ACTED_IN`" AND NOT propertyName IN allowedEdgeProperties.ACTED_IN
OR relType = ":`DIRECTED`" AND NOT propertyName IN allowedEdgeProperties.DIRECTED
OR relType = ":`RATED`" AND NOT propertyName IN allowedEdgeProperties.RATED
OR relType = ":`IN_GENRE`" AND NOT propertyName IN allowedEdgeProperties.IN_GENRE
RETURN relType, propertyName;

// Find node properties with the wrong datatype
CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, propertyTypes
WHERE "Movie" IN nodeLabels AND (
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
RETURN nodeLabels, propertyName, propertyTypes;

// Find edge properties with the wrong datatype
CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes
WHERE relType = ":`ACTED_IN`" AND propertyName = "role" AND propertyTypes <> ["String"]
OR relType = ":`DIRECTED`" AND propertyName = "role" AND propertyTypes <> ["String"]
OR relType = ":`RATED`" AND (
    propertyName = "rating" AND propertyTypes <> ["Double"]
    OR propertyName = "timestamp" AND propertyTypes <> ["Long"]
)
RETURN relType, propertyName, propertyTypes;

// Check for edges between wrong types of nodes
// We can't do this with the schema statistics, because this finds that some Actors have DIRECTED eges, which is allowed, but only when they are also Directors
MATCH (n)-[e:ACTED_IN]->(m) WHERE NOT "Actor" IN labels(n) OR NOT "Movie" IN labels(m) RETURN e;
MATCH (n)-[e:DIRECTED]->(m) WHERE NOT "Director" IN labels(n) OR NOT "Movie" IN labels(m) RETURN e;
MATCH (n)-[e:IN_GENRE]->(m) WHERE NOT "Movie" IN labels(n) OR NOT "Genre" IN labels(m) RETURN e;
MATCH (n)-[e:RATED]->(m) WHERE NOT "User" IN labels(n) OR NOT "Movie" IN labels(m) RETURN e;

// Check for missing mandatory edges
MATCH (a:Actor) WHERE NOT (a)-[:ACTED_IN]->(:Movie) RETURN count(a) > 0;
MATCH (d:Director) WHERE NOT (d)-[:DIRECTED]->(:Movie) RETURN count(d) > 0;
MATCH (m:Movie) WHERE NOT (m)-[:IN_GENRE]->(:Genre) RETURN count(g) > 0;
