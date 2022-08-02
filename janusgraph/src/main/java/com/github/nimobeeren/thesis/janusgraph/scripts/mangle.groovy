// These statements are meant to be run manually to introduce errors into the data

// Single node
g.V().sample(1).property("id", null)
g.tx().commit()

// Half of all nodes
numV = g.V().count().next()
g.V().limit((int) (numV / 2)).property("id", null)
g.tx().commit()
