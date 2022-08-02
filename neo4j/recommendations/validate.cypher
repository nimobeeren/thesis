// This set of queries can be used to check if the graph conforms to the
// recommendations schema.
// Each query returns the graph objects that violate one of the rules.
// It does not check for things that can be avoided using Neo4j's built-in
// constraints, such as missing mandatory properties.

// Find all violating nodes
WITH
[
    ["Movie"], ["Genre"], ["User"], ["Actor", "Person"],
    ["Director", "Person"], ["Actor", "Director", "Person"]
] AS allowedNodeLabelSets,
{
    Movie: ["budget", "countries", "imdbId", "imdbRating", "imdbVotes", "languages", "movieId", "plot", "poster", "released", "revenue", "runtime", "title", "tmdbId", "url", "year"],
    Person: ["bio", "born", "bornIn", "died", "imdbId", "name", "poster", "tmdbId", "url"],
    User: ["userId", "name"],
    Genre: ["name"]
} AS allowedNodeProperties,
{
    languages: "String[]",
    year: "INTEGER",
    imdbId: "STRING",
    runtime: "INTEGER",
    imdbRating: "FLOAT",
    movieId: "STRING",
    countries: "String[]",
    imdbVotes: "INTEGER",
    title: "STRING",
    url: "STRING",
    revenue: "INTEGER",
    tmdbId: "STRING",
    plot: "STRING",
    poster: "STRING",
    released: "STRING",
    budget: "INTEGER",
    bio: "STRING",
    bornIn: "STRING",
    born: "LocalDate",
    name: "STRING",
    died: "LocalDate",
    userId: "STRING"
} AS nodePropertyKeys
MATCH (n)
// Find all nodes that have a label set that is not allowed
// NOTE: this depends on the order of labels in the list, but it seems to be consistent
WHERE NOT labels(n) IN allowedNodeLabelSets
// Find all nodes that have a property that is not allowed
// Note that this doesn't work if a node has more than one of these labels
OR "Movie" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Movie)
OR "Person" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Person)
OR "User" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.User)
OR "Genre" IN labels(n) AND NOT all(propertyKey IN keys(n) WHERE propertyKey IN allowedNodeProperties.Genre)
// Find all nodes that have a property with the wrong datatype
// This assumes that if two properties have the same key, they have the same datatype,
// i.e. two node types can't have the same property key with different datatype
OR NOT all(propertyKey IN keys(n) WHERE apoc.meta.type(n[propertyKey]) = nodePropertyKeys[propertyKey])
// Check for missing mandatory edges
OR "Actor" IN labels(n) AND NOT (n)-[:ACTED_IN]->(:Movie)
OR "Director" IN labels(n) AND NOT (n)-[:DIRECTED]->(:Movie)
OR "Movie" IN labels(n) AND NOT (n)-[:IN_GENRE]->(:Genre)
RETURN n;


// Find all violating edges
WITH
["IN_GENRE", "RATED", "ACTED_IN", "DIRECTED"] AS allowedLabels,
{
    ACTED_IN: ["role"],
    DIRECTED: ["role"],
    RATED: ["rating", "timestamp"],
    IN_GENRE: []
} AS allowedEdgeProperties,
{
    role: "STRING",
    rating: "FLOAT",
    timestamp: "INTEGER"
} AS edgePropertyKeys
MATCH (n)-[e]->(m)
// Find all edges that have a label that is not allowed
WHERE NOT type(e) IN allowedLabels
// Find edge properties that are not allowed
OR type(e) = "ACTED_IN" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.ACTED_IN)
OR type(e) = "DIRECTED" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.DIRECTED)
OR type(e) = "RATED" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.RATED)
OR type(e) = "IN_GENRE" AND NOT all(propertyKey IN keys(e) WHERE propertyKey IN allowedEdgeProperties.IN_GENRE)
// Find all edges that have a property with the wrong datatype
// This has the same limitations as with nodes
OR NOT all(propertyKey IN keys(e) WHERE apoc.meta.type(e[propertyKey]) = edgePropertyKeys[propertyKey])
// Check for edges between wrong types of nodes
OR type(e) = "ACTED_IN" AND (NOT "Actor" IN labels(n) OR NOT labels(m) = ["Movie"])
OR type(e) = "DIRECTED" AND (NOT "Director" IN labels(n) OR NOT labels(m) = ["Movie"])
OR type(e) = "IN_GENRE" AND (NOT labels(n) = ["Movie"] OR NOT labels(m) = ["Genre"])
OR type(e) = "RATED" AND (NOT labels(n) = ["User"] OR NOT labels(m) = ["Movie"])
RETURN e;
