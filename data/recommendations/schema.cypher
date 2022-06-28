CREATE
(movie:Schema:Movie {
  budget: "INTEGER?",
  countries: "String[]?",
  imdbId: "STRING",
  imdbRating: "FLOAT?",
  imdbVotes: "INTEGER?",
  languages: "String[]?",
  movieId: "STRING",
  plot: "STRING?",
  poster: "STRING?",
  released: "STRING?",
  revenue: "INTEGER?",
  runtime: "INTEGER?",
  title: "STRING",
  tmdbId: "STRING?",
  url: "STRING?",
  year: "INTEGER?"
}),
(actor:Schema:Actor { 
  bio: "STRING?",
  born: "LocalDate?",
  bornIn: "STRING?",
  died: "LocalDate?",
  imdbId: "STRING?",
  name: "STRING",
  poster: "STRING?",
  tmdbId: "STRING",
  url: "STRING"
}),
(director:Schema:Director {
  bio: "STRING?",
  born: "LocalDate?",
  bornIn: "STRING?",
  died: "LocalDate?",
  imdbId: "STRING?",
  name: "STRING",
  poster: "STRING?",
  tmdbId: "STRING",
  url: "STRING"
}),
(actorDirector:Schema:Actor:Director {
  bio: "STRING?",
  born: "LocalDate?",
  bornIn: "STRING?",
  died: "LocalDate?",
  imdbId: "STRING?",
  name: "STRING",
  poster: "STRING?",
  tmdbId: "STRING",
  url: "STRING"
}),
(user:Schema:User {
  name: "STRING",
  userId: "STRING"
}),
(genre:Schema:Genre {
  genre: "STRING"
}),
(actor)-[:ACTED_IN {__outMin: 1, role: "STRING?"}]->(movie),
(actorDirector)-[:ACTED_IN {__outMin: 1, role: "STRING?"}]->(movie),
(director)-[:DIRECTED {__outMin: 1, role: "STRING?"}]->(movie),
(actorDirector)-[:DIRECTED {__outMin: 1, role: "STRING?"}]->(movie),
(user)-[:RATED {rating: "FLOAT", timestamp: "INTEGER"}]->(movie),
(movie)-[:IN_GENRE {__outMin: 1}]->(genre)

RETURN *
