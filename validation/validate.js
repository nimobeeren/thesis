/**
 * Finds the nodes that do not match the labels or properties of any node type in the schema.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that do not conform to any node type
 */
async function validateNodes(session) {
  return await session.run(`
    MATCH (dataNode:Data)
    WHERE NOT EXISTS {
      MATCH (schemaNode:Schema)
      WHERE apoc.meta.types(dataNode) = properties(schemaNode)
      AND [label IN labels(dataNode) WHERE label <> "Data"] = [label IN labels(schemaNode) WHERE label <> "Schema"]
    }
    RETURN dataNode
  `);
}

/**
 * Finds the edges that do not match the label or properties of any edge type in the schema.
 * @param {import('neo4j-driver').Session} session
 * @returns all data edges that do not conform to any edge type
 */
async function validateEdges(session) {
  return await session.run(`
    MATCH (:Data)-[dataEdge]->(:Data)
    WHERE NOT EXISTS {
      MATCH (:Schema)-[schemaEdge]->(:Schema)
      WHERE apoc.meta.types(dataEdge) = properties(schemaEdge)
      AND type(dataEdge) = type(schemaEdge)
    }
    RETURN dataEdge
  `);
}

/**
 * Finds the nodes that are missing an incoming edge.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that are missing an incoming edge of a particular type
 */
async function validateIncomingEdges(session) {
  return await session.run(`
    MATCH (sn1:Schema)-[se]->(sn2:Schema)
    MATCH (dn2:Data)
    WHERE apoc.meta.types(dn2) = properties(sn2)
    AND [label IN labels(dn2) WHERE label <> "Data"] = [label IN labels(sn2) WHERE label <> "Schema"]
    AND NOT EXISTS {
      MATCH (dn1:Data)-[de]->(dn2)
      WHERE apoc.meta.types(de) = properties(se)
      AND type(de) = type(se)
      AND apoc.meta.types(dn1) = properties(sn1)
      AND [label IN labels(dn1) WHERE label <> "Data"] = [label IN labels(sn1) WHERE label <> "Schema"]
    }
    RETURN dn2
  `);
}

/**
 * Finds the nodes that are missing an outgoing edge.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that are missing an outgoing edge of a particular type
 */
async function validateOutgoingEdges(session) {
  return await session.run(`
    MATCH (sn1:Schema)-[se]->(sn2:Schema)
    MATCH (dn1:Data)
    WHERE apoc.meta.types(dn1) = properties(sn1)
    AND [label IN labels(dn1) WHERE label <> "Data"] = [label IN labels(sn1) WHERE label <> "Schema"]
    AND NOT EXISTS {
      MATCH (dn1)-[de]->(dn2:Data)
      WHERE apoc.meta.types(de) = properties(se)
      AND type(de) = type(se)
      AND apoc.meta.types(dn2) = properties(sn2)
      AND [label IN labels(dn2) WHERE label <> "Data"] = [label IN labels(sn2) WHERE label <> "Schema"]
    }
    RETURN dn1
  `);
}

module.exports = {
  validateNodes,
  validateEdges,
  validateIncomingEdges,
  validateOutgoingEdges,
};
