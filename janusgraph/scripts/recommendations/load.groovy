mgmt = graph.openManagement()
mgmt.set("storage.batch-loading", true)
mgmt.commit()

// LEFT HERE
// Can't figure out how to import and use a CSV library in the Gremlin console
// Maybe try janusgraph-csv-import to see if it works at all?

tx = graph.newTransaction()

toyStory = tx.addVertex(T.label, "Movie", "imdbId", "114709", "movieId", "1", "title", "Toy Story")
animation = tx.addVertex(T.label, "Genre", "genre", "Animation")
toyStory.addEdge("IN_GENRE", animation)

tx.commit()
