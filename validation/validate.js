/**
 * Finds the nodes that do not match the labels or properties of any node type in the schema.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that do not conform to any node type.
 */
async function validateNodes(session) {
  return await session.run(`
    MATCH (dn:Data)
    WHERE NOT EXISTS {
      MATCH (sn:Schema)

      // dn conforms to sn
      WHERE apoc.meta.types(dn) = properties(sn)
      AND [label IN labels(dn) WHERE label <> "Data"] = [label IN labels(sn) WHERE label <> "Schema"]
    }
    RETURN dn
  `);
}

/**
 * Finds the edges that do not match the label or properties of any edge type in the schema.
 * @param {import('neo4j-driver').Session} session
 * @returns all data edges that do not conform to any edge type.
 */
async function validateEdges(session) {
  return await session.run(`
    MATCH (dn1:Data)-[de]->(dn2:Data)
    WHERE NOT EXISTS {
        MATCH (sn1:Schema)-[se]->(sn2:Schema)

        // de conforms to se
        WHERE apoc.meta.types(de) = apoc.map.clean(properties(se), ["__inMin", "__inMax", "__outMin", "__outMax"], [])
        AND type(de) = type(se)
    
        // dn1 conforms to sn1
        AND apoc.meta.types(dn1) = properties(sn1)
        AND [label IN labels(dn1) WHERE label <> "Data"] = [label IN labels(sn1) WHERE label <> "Schema"]
    
        // dn2 conforms to sn2
        AND apoc.meta.types(dn2) = properties(sn2)
        AND [label IN labels(dn2) WHERE label <> "Data"] = [label IN labels(sn2) WHERE label <> "Schema"]
    }
    RETURN de
  `);
}

/**
 * Finds the nodes that are missing an incoming edge.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that don't have the right number of incoming edges of
 * a particular type.
 */
async function validateIncomingEdges(session) {
  return await session.run(`
    MATCH (sn1:Schema)-[se]->(sn2:Schema)
    MATCH (dn2:Data)
    
    WITH sn1, se, sn2, dn2,
    // default cardinality constraint is 0..*
    coalesce(se.__inMin, 0) AS inMin,
    coalesce(se.__inMax, apoc.math.maxLong()) AS inMax
    
    // dn2 conforms to sn2
    WHERE apoc.meta.types(dn2) = properties(sn2)
    AND [label IN labels(dn2) WHERE label <> "Data"] = [label IN labels(sn2) WHERE label <> "Schema"]
    
    // se has a cardinality constraint on incoming edges
    AND (inMin > 0 OR inMax < apoc.math.maxLong())
    
    // the cardinality constraint of se is violated
    AND NOT inMin <= size([
      (dn1:Data)-[de]->(dn2)
  
      // de conforms to se
      WHERE apoc.meta.types(de) = apoc.map.clean(properties(se), ["__inMin", "__inMax", "__outMin", "__outMax"], [])
      AND type(de) = type(se)
  
      // dn1 conforms to sn1
      AND apoc.meta.types(dn1) = properties(sn1)
      AND [label IN labels(dn1) WHERE label <> "Data"] = [label IN labels(sn1) WHERE label <> "Schema"]
    | dn1]) <= inMax
    
    RETURN dn2
  `);
}

/**
 * Finds the nodes that are missing an outgoing edge.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that don't have the right number of outgoing edges of
 * a particular type.
 */
async function validateOutgoingEdges(session) {
  // FIXME: always returns empty when outMin or outMax is null
  return await session.run(`
    MATCH (sn1:Schema)-[se]->(sn2:Schema)
    MATCH (dn1:Data)
    
    WITH sn1, se, sn2, dn1,
    // default cardinality constraint is 0..*
    coalesce(se.__outMin, 0) AS outMin,
    coalesce(se.__outMax, apoc.math.maxLong()) AS outMax
    
    // dn1 conforms to sn1
    WHERE apoc.meta.types(dn1) = properties(sn1)
    AND [label IN labels(dn1) WHERE label <> "Data"] = [label IN labels(sn1) WHERE label <> "Schema"]
    
    // se has a cardinality constraint on outgoing edges
    AND (outMin > 0 OR outMax < apoc.math.maxLong())
    
    // the cardinality constraint of se is violated
    AND NOT outMin <= size([
      (dn1)-[de]->(dn2:Data)
  
      // de conforms to se
      WHERE apoc.meta.types(de) = apoc.map.clean(properties(se), ["__inMin", "__inMax", "__outMin", "__outMax"], [])
      AND type(de) = type(se)
  
      // dn2 conforms to sn2
      AND apoc.meta.types(dn2) = properties(sn2)
      AND [label IN labels(dn2) WHERE label <> "Data"] = [label IN labels(sn2) WHERE label <> "Schema"]
    | dn2]) <= outMax
    
    RETURN dn1
  `);
}

module.exports = {
  validateNodes,
  validateEdges,
  validateIncomingEdges,
  validateOutgoingEdges,
};
