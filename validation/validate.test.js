require("dotenv/config");
const neo4j = require("neo4j-driver");
const {
  validateNodes,
  validateEdges,
  validateIncomingEdges,
  validateOutgoingEdges,
} = require("./validate");

const URI = process.env.NEO4J_URI || "bolt://localhost:7687";
const USERNAME = process.env.NEO4J_USERNAME || "neo4j";
const PASSWORD = process.env.NEO4J_PASSWORD;

if (!PASSWORD) {
  console.error(
    "No password supplied, please set the environment variable NEO4J_PASSWORD"
  );
  process.exit(1);
}

// Connect to the database
const driver = neo4j.driver(URI, neo4j.auth.basic(USERNAME, PASSWORD));
const session = driver.session();

// Make sure we're working with a clean DB from the start
beforeAll(async () => {
  const result = await session.writeTransaction((tx) =>
    tx.run("MATCH (n) RETURN n")
  );

  if (result.records.length > 0) {
    throw new Error("Database is not empty! Aborting to prevent data loss.");
  }
});

// Clean up anything we created during a test
afterEach(async () => {
  // Delete all nodes and edges in the database
  return await session.writeTransaction((tx) =>
    tx.run("MATCH (n) DETACH DELETE n")
  );
});

// Close DB connection
afterAll(async () => {
  await session.close();
  await driver.close();
});

test("the empty graph conforms to the empty schema", async () => {
  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("graph conforms to schema", async () => {
  // Create the schema
  await session.writeTransaction((tx) =>
    tx.run(`
      CREATE (user:Schema:User { name: "String", age: "Integer" })-[:FOLLOWS { since: "Date" }]->(user),
      (user)-[:LIKES]->(post:Schema:Post { content: "String", createdAt: "Date" });
    `)
  );

  // Create the data
  await session.writeTransaction((tx) =>
    tx.run(`
      CREATE (user1:Data:User { name: "String", age: "Integer" }),
      (user2:Data:User { name: "String", age: "Integer" }),
      (user1)-[:FOLLOWS {since: "Date"}]->(user2),
      (user2)-[:FOLLOWS {since: "Date"}]->(user1),
      (user1)-[:LIKES]->(post1:Data:Post { content: "String", createdAt: "Date" }),
      (user2)-[:LIKES]->(post2:Data:Post { content: "String", createdAt: "Date" });
    `)
  );

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});
