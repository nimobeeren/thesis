// Find node labels that are not allowed
CALL db.labels() YIELD label AS allLabels
RETURN all(label IN collect(allLabels) WHERE label IN ["Movie", "Genre", "User", "Actor", "Director", "Person"]);
// Find edge labels that are not allowed
CALL db.relationshipTypes() YIELD relationshipType AS allTypes
RETURN all(type IN collect(allTypes) WHERE type IN ["IN_GENRE", "RATED", "ACTED_IN", "DIRECTED"]);

// Find node properties that are not allowed
WITH ["budget", "countries", "imdbId", "imdbRating", "imdbVotes", "languages", "movieId", "plot", "poster", "released", "revenue", "runtime", "title", "tmdbId", "url", "year"] AS movieAllowedProperties,
["bio", "born", "bornIn", "died", "imdbId", "name", "poster", "tmdbId", "url"] AS personAllowedProperties,
["userId", "name"] AS userAllowedProperties,
["name"] AS genreAllowedProperties
CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName
WHERE "Movie" IN nodeLabels AND NOT propertyName IN movieAllowedProperties
OR "Person" IN nodeLabels AND NOT propertyName IN personAllowedProperties
OR "User" IN nodeLabels AND NOT propertyName IN userAllowedProperties
OR "Genre" IN nodeLabels AND NOT propertyName IN genreAllowedProperties
RETURN nodeLabels, propertyName;

// Find edge properties that are not allowed
WITH ["role"] AS actedInAllowedProperties,
["role"] AS directedAllowedProperties,
["rating", "timestamp"] AS ratedAllowedProperties,
[null] AS inGenreAllowedProperties
CALL db.schema.relTypeProperties() YIELD relType, propertyName
WHERE relType = ":`ACTED_IN`" AND NOT propertyName IN actedInAllowedProperties
OR relType = ":`DIRECTED`" AND NOT propertyName IN directedAllowedProperties
OR relType = ":`RATED`" AND NOT propertyName IN ratedAllowedProperties
OR relType = ":`IN_GENRE`" AND NOT propertyName IN inGenreAllowedProperties
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

// FIXME: this finds that some Actors have DIRECTED eges, which is allowed, but only when they are also Directors
// Check for edges between wrong types of nodes
CALL apoc.meta.schema() YIELD value AS schema
WITH schema,
{out: ["IN_GENRE"], in: ["ACTED_IN", "DIRECTED", "RATED"]} AS movieAllowedEdges,
{out: ["ACTED_IN"], in: []} AS actorAllowedEdges,
{out: ["DIRECTED"], in: []} AS directorAllowedEdges,
{out: ["RATED"], in: []} AS userAllowedEdges,
{out: [], in: ["IN_GENRE"]} AS genreAllowedEdges
RETURN size([
    relType IN keys(schema.Movie.relationships)
    WHERE schema.Movie.relationships[relType].direction = "out" AND NOT relType IN movieAllowedEdges.out
    OR schema.Movie.relationships[relType].direction = "in" AND NOT relType IN movieAllowedEdges.in
]) > 0
OR size([
    relType IN keys(schema.Actor.relationships)
    WHERE schema.Actor.relationships[relType].direction = "out" AND NOT relType IN actorAllowedEdges.out
    OR schema.Actor.relationships[relType].direction = "in" AND NOT relType IN actorAllowedEdges.in
]) > 0
OR size([
    relType IN keys(schema.Director.relationships)
    WHERE schema.Director.relationships[relType].direction = "out" AND NOT relType IN directorAllowedEdges.out
    OR schema.Director.relationships[relType].direction = "in" AND NOT relType IN directorAllowedEdges.in
]) > 0
OR size([
    relType IN keys(schema.User.relationships)
    WHERE schema.User.relationships[relType].direction = "out" AND NOT relType IN userAllowedEdges.out
    OR schema.User.relationships[relType].direction = "in" AND NOT relType IN userAllowedEdges.in
]) > 0
OR size([
    relType IN keys(schema.Genre.relationships)
    WHERE schema.Genre.relationships[relType].direction = "out" AND NOT relType IN genreAllowedEdges.out
    OR schema.Genre.relationships[relType].direction = "in" AND NOT relType IN genreAllowedEdges.in
]) > 0;

// Check for missing mandatory edges
MATCH (a:Actor) WHERE NOT (a)-[:ACTED_IN]->(:Movie) RETURN count(a) > 0;
MATCH (d:Director) WHERE NOT (d)-[:DIRECTED]->(:Movie) RETURN count(d) > 0;
MATCH (m:Movie) WHERE NOT (m)-[:IN_GENRE]->(:Genre) RETURN count(g) > 0;
