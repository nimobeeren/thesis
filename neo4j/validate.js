const macros = {
  nodeConforms(dn, sn) {
    return `
      // all labels match, except Data and Schema labels
      [label IN labels(${dn}) WHERE label <> "Data"] = [label IN labels(${sn}) WHERE label <> "Schema"]
      // all properties of dn are specified in sn and the right type
      AND all(k IN keys(${dn}) WHERE ${sn}[k] IS NOT NULL AND apoc.meta.type(${dn}[k]) = replace(${sn}[k], "?", ""))
      // all mandatory properties of sn are present in dn
      AND all(k in keys(${sn}) WHERE ${sn}[k] ENDS WITH "?" OR ${dn}[k] IS NOT NULL)
    `;
  },
  edgeConforms(de, se) {
    return `
      // label matches
      type(${de}) = type(${se})
      // all properties of de are specified in se and the right type
      AND all(k IN keys(${de}) WHERE ${se}[k] IS NOT NULL AND apoc.meta.type(${de}[k]) = replace(${se}[k], "?", ""))
      // all mandatory properties of se are present in de
      AND all(k IN keys(${se}) WHERE k IN ["__inMin", "__inMax", "__outMin", "__outMax"] OR ${se}[k] ENDS WITH "?" OR ${de}[k] IS NOT NULL)
    `;
  },
};

/**
 * Finds the nodes that do not match the labels or properties of any node type in the schema.
 * @param {import('neo4j-driver').Session} session
 * @returns all data nodes that do not conform to any node type.
 */
function validateNodes(session) {
  return session.run(`
    MATCH (dn:Data)
    WHERE NOT EXISTS {
      MATCH (sn:Schema)

      WHERE ${macros.nodeConforms("dn", "sn")}
    }
    RETURN dn
  `);
}

/**
 * Finds the edges that do not match the label or properties of any edge type in the schema.
 * @param {import('neo4j-driver').Session} session
 * @returns all data edges that do not conform to any edge type.
 */
function validateEdges(session) {
  return session.run(`
    MATCH (dn1:Data)-[de]->(dn2:Data)
    WHERE NOT EXISTS {
      MATCH (sn1:Schema)-[se]->(sn2:Schema)
  
      WHERE ${macros.edgeConforms("de", "se")}
      AND ${macros.nodeConforms("dn1", "sn1")}
      AND ${macros.nodeConforms("dn2", "sn2")}
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
function validateIncomingEdges(session) {
  return session.run(`
    MATCH (sn1:Schema)-[se]->(sn2:Schema)
    MATCH (dn2:Data)
    
    WITH sn1, se, sn2, dn2,
    // default cardinality constraint is 0..*
    coalesce(se.__inMin, 0) AS inMin,
    coalesce(se.__inMax, apoc.math.maxLong()) AS inMax
    
    WHERE ${macros.nodeConforms("dn2", "sn2")}
    
    // se has a cardinality constraint on incoming edges
    AND (inMin > 0 OR inMax < apoc.math.maxLong())
    
    // the cardinality constraint of se is violated
    AND NOT inMin <= size([
      (dn1:Data)-[de]->(dn2)
      WHERE ${macros.edgeConforms("de", "se")}
      AND ${macros.nodeConforms("dn1", "sn1")}
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
function validateOutgoingEdges(session) {
  return session.run(`
    MATCH (sn1:Schema)-[se]->(sn2:Schema)
    MATCH (dn1:Data)
    
    WITH sn1, se, sn2, dn1,
    // default cardinality constraint is 0..*
    coalesce(se.__outMin, 0) AS outMin,
    coalesce(se.__outMax, apoc.math.maxLong()) AS outMax
    
    WHERE ${macros.nodeConforms("dn1", "sn1")}
    
    // se has a cardinality constraint on outgoing edges
    AND (outMin > 0 OR outMax < apoc.math.maxLong())
    
    // the cardinality constraint of se is violated
    AND NOT outMin <= size([
      (dn1)-[de]->(dn2:Data)
      WHERE ${macros.edgeConforms("de", "se")}
      AND ${macros.nodeConforms("dn2", "sn2")}
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
