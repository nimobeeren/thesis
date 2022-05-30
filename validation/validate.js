/**
 * Checks the vocabulary for nodes.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that do not conform to any node type in the schema
 */
async function validateNodes(session) {
  return await session.writeTransaction((tx) =>
    tx.run(`
      MATCH (dataNode:Data)
      WHERE NOT EXISTS {
          MATCH (schemaNode:Schema)
          WHERE properties(dataNode) = properties(schemaNode)
          AND [label IN labels(dataNode) WHERE label <> "Data"] = [label IN labels(schemaNode) WHERE label <> "Schema"]
      }
      RETURN dataNode
    `)
  );
}

/**
 * Checks the vocabulary for edges.
 * @param {import('neo4j-driver').Session} session
 * @returns all data edges that do not conform to any edge type in the schema
 */
async function validateEdges(session) {
  return await session.writeTransaction((tx) =>
    tx.run(`
      MATCH (:Data)-[dataEdge]->(:Data)
      WHERE NOT EXISTS {
          MATCH (:Schema)-[schemaEdge]->(:Schema)
          WHERE properties(dataEdge) = properties(schemaEdge)
          AND type(dataEdge) = type(schemaEdge)
      }
      RETURN dataEdge
    `)
  );
}

/**
 * Finds the nodes that are missing an incoming edge.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that are missing an incoming edge of a particular type
 */
async function validateIncomingEdges(session) {
  return await session.writeTransaction((tx) =>
    tx.run(`
      MATCH (sn1:Schema)-[se]->(sn2:Schema)
      MATCH (dn2:Data)
      WHERE properties(dn2) = properties(sn2)
      AND [label IN labels(dn2) WHERE label <> "Data"] = [label IN labels(sn2) WHERE label <> "Schema"]
      AND NOT EXISTS {
          MATCH (dn1:Data)-[de]->(dn2)
          WHERE properties(de) = properties(se)
          AND type(de) = type(se)
          AND properties(dn1) = properties(sn1)
          AND [label IN labels(dn1) WHERE label <> "Data"] = [label IN labels(sn1) WHERE label <> "Schema"]
      }
      RETURN dn2
    `)
  );
}

/**
 * Finds the nodes that are missing an outgoing edge.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that are missing an outgoing edge of a particular type
 */
async function validateOutgoingEdges(session) {
  return await session.writeTransaction((tx) =>
    tx.run(`
      MATCH (sn1:Schema)-[se]->(sn2:Schema)
      MATCH (dn1:Data)
      WHERE properties(dn1) = properties(sn1)
      AND [label IN labels(dn1) WHERE label <> "Data"] = [label IN labels(sn1) WHERE label <> "Schema"]
      AND NOT EXISTS {
          MATCH (dn1)-[de]->(dn2:Data)
          WHERE properties(de) = properties(se)
          AND type(de) = type(se)
          AND properties(dn2) = properties(sn2)
          AND [label IN labels(dn2) WHERE label <> "Data"] = [label IN labels(sn2) WHERE label <> "Schema"]
      }
      RETURN dn1
    `)
  );
}

module.exports = {
  validateNodes,
  validateEdges,
  validateIncomingEdges,
  validateOutgoingEdges,
};
