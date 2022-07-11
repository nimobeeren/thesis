graph = JanusGraphFactory.open("inmemory")

mgmt = graph.openManagement()

mgmt.makeVertexLabel("Movie").make()
mgmt.makeVertexLabel("Genre").make()

mgmt.makeEdgeLabel("IN_GENRE").make()

mgmt.makePropertyKey("imdbId").dataType(String).make()
mgmt.makePropertyKey("movieId").dataType(String).make()
mgmt.makePropertyKey("title").dataType(String).make()
mgmt.makePropertyKey("genre").dataType(String).make()

mgmt.commit()
