// These queries meant to be run manually to introduce errors into the data

// Remove mandatory edges from a single random node
CALL {
    MATCH (n:Message)
    RETURN n, rand() as r
    ORDER BY r
    LIMIT 1
}
WITH n
MATCH (n)-[e:HAS_CREATOR]->()
DELETE e
RETURN id(n)

// Remove mandatory edges from half of all nodes
MATCH (n:Message)-[e:HAS_CREATOR]->()
WHERE rand() > 0.5
DELETE e
RETURN id(n)
