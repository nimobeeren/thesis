require("dotenv/config");
const neo4j = require("neo4j-driver");
const { validateNodes, validateEdges, validateIncomingEdges, validateOutgoingEdges } = require("./validate");

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

/**
 * Deletes all nodes and edges in the DB.
 */
async function clearDatabase() {
  return await session.writeTransaction((tx) =>
    tx.run("MATCH (n) DETACH DELETE n")
  );
}

// Make sure we're working with a clean DB from the start
beforeAll(async () => {
  await clearDatabase();
});

// Clean up anything we created during a test
afterEach(async () => {
  await clearDatabase();
});

// Close DB connection
afterAll(async () => {
  await session.close();
  await driver.close();
});

describe("validateNodes()", () => {
  test("the empty graph conforms to the empty schema", async () => {
    const result = await validateNodes(session);
    expect(result.records).toHaveLength(0);
  });
});

describe("validateEdges()", () => {
  test("the empty graph conforms to the empty schema", async () => {
    const result = await validateEdges(session);
    expect(result.records).toHaveLength(0);
  });
});

describe("validateIncomingEdges()", () => {
  test("the empty graph conforms to the empty schema", async () => {
    const result = await validateIncomingEdges(session);
    expect(result.records).toHaveLength(0);
  });
});

describe("validateOutgoingEdges()", () => {
  test("the empty graph conforms to the empty schema", async () => {
    const result = await validateOutgoingEdges(session);
    expect(result.records).toHaveLength(0);
  });
});
