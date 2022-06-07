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
  const result = await session.run("MATCH (n) RETURN n");

  if (result.records.length > 0) {
    throw new Error("Database is not empty! Aborting to prevent data loss.");
  }
});

// Clean up anything we created during a test
afterEach(async () => {
  // Delete all nodes and edges in the database
  return await session.run("MATCH (n) DETACH DELETE n");
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
  await session.run(`
    CREATE (user:Schema:User { name: "STRING", age: "INTEGER" })-[:FOLLOWS { since: "ZonedDateTime" }]->(user),
    (user)-[:LIKES]->(post:Schema:Post { content: "STRING", createdAt: "ZonedDateTime" })
  `);

  // Create the data
  await session.run(`
    CREATE (user1:Data:User { name: "Francesca", age: 42 }),
    (user2:Data:User { name: "Rose", age: 24 }),
    (user1)-[:FOLLOWS {since: datetime("2022-02-27")}]->(user2),
    (user2)-[:FOLLOWS {since: datetime("2022-02-27")}]->(user1),
    (user1)-[:LIKES]->(post1:Data:Post { content: "Hello", createdAt: datetime("2022-05-30T13:37:01") }),
    (user2)-[:LIKES]->(post2:Data:Post { content: "World", createdAt: datetime("2022-05-31T5:42:02") })
  `);

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("node is missing mandatory property", async () => {
  // Create the schema
  await session.run(
    `CREATE (user:Schema:User { name: "STRING", age: "INTEGER" })`
  );

  // Create the data
  const dataResult = await session.run(
    `CREATE (user:Data:User { name: "Rose" }) RETURN user`
  );

  const userNodeId = dataResult.records[0].get("user").identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(1);
  expect(violatingNodes.records[0].get(0).identity).toEqual(userNodeId);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("edge is missing mandatory property", async () => {
  // Create the schema
  await session.run(`
    CREATE (:Schema:User { name: "STRING", age: "INTEGER" })-[:CREATED { at: "ZonedDateTime" }]->
    (:Schema:Post { content: "STRING" })
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (user:Data:User { name: "Nick", age: 38 })-[created:CREATED]->
    (post:Data:Post { content: "Diggity" })
    RETURN user, created, post
  `);

  const userNodeId = dataResult.records[0].get("user").identity;
  const createdEdgeId = dataResult.records[0].get("created").identity;
  const postNodeId = dataResult.records[0].get("post").identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  // The CREATED edge is missing the at property
  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(1);
  expect(violatingEdges.records[0].get(0).identity).toEqual(createdEdgeId);

  // Since the CREATED edge does not conform, the Post node also doesn't
  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(1);
  expect(violatingIncomingEdges.records[0].get(0).identity).toEqual(postNodeId);

  // Since the CREATED edge does not conform, the User node also doesn't
  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(1);
  expect(violatingOutgoingEdges.records[0].get(0).identity).toEqual(userNodeId);
});

test("node is missing incoming edge", async () => {
  // Create the schema
  await session.run(`
    CREATE (:Schema:User { name: "STRING", age: "INTEGER" })-[:CREATED]->
    (:Schema:Post { content: "STRING" })
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (:Data:User { name: "Francesca", age: 42 })-[:CREATED]->(:Data:Post { content: "🍔" }),
    (post:Data:Post { content: "🥬" })
    RETURN post
  `);

  const postNodeId = dataResult.records[0].get("post").identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  // The Post node does not have an incoming CREATED edge from a User
  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(1);
  expect(violatingIncomingEdges.records[0].get(0).identity).toEqual(postNodeId);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("node is missing outgoing edge", async () => {
  // Create the schema
  await session.run(`
    CREATE (:Schema:User { name: "STRING", age: "INTEGER" })-[:CREATED]->
    (:Schema:Post { content: "STRING" })
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (:Data:User { name: "Francesca", age: 42 })-[:CREATED]->(:Data:Post { content: "🐑" }),
    (user:Data:User { name: "Rose", age: 24 })
    RETURN user
  `);

  const userNodeId = dataResult.records[0].get("user").identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  // The User node has no outgoing CREATED edge to a Post
  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(1);
  expect(violatingOutgoingEdges.records[0].get(0).identity).toEqual(userNodeId);
});

test("edge has wrong target node", async () => {
  // Create the schema
  await session.run(`
    CREATE (:Schema:User)-[:CREATED]->(:Schema:Post)
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (user:Data:User)-[:CREATED]->(:Data:Post),
    (user)-[selfloop:CREATED]->(user)
    RETURN selfloop
  `);

  const userNodeId = dataResult.records[0].get(0).identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(1);
  expect(violatingEdges.records[0].get(0).identity).toEqual(userNodeId);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("edge has wrong source node", async () => {
  // Create the schema
  await session.run(`
    CREATE (:Schema:User)-[:CREATED]->(:Schema:Post)
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (:Data:User)-[:CREATED]->(post:Data:Post),
    (post)-[selfloop:CREATED]->(post)
    RETURN selfloop
  `);

  const userNodeId = dataResult.records[0].get(0).identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(1);
  expect(violatingEdges.records[0].get(0).identity).toEqual(userNodeId);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("self-loops allow any edge between nodes of that type", async () => {
  // Create the schema
  await session.run(`
    CREATE (user:Schema:User)-[:KNOWS]->(user)
  `);

  // Create the data
  await session.run(`
    CREATE (user1:Data:User)-[:KNOWS]->(user2:Data:User),
    (user2)-[:KNOWS]->(user1),
    (user2)-[:KNOWS]->(user2)
  `);

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("cardinality constraint is violated", async () => {
  // Create the schema
  // Users can create 0 or 1 Posts, and every Post is created by exactly 1 User
  await session.run(`
    CREATE (:Schema:User)-[:CREATED {srcMin: 1, srcMax: 1, trgMin: 0, trgMax: 1}]->(:Schema:Post)
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (:Data:User),
    (:Data:User)-[:CREATED]->(:Data:Post),
    (:Data:Post)<-[:CREATED]-(tooManyPostsUser:Data:User)-[:CREATED]->(:Data:Post),
    (noCreatorPost:Data:Post)
    RETURN tooManyPostsUser, noCreatorPost
  `);

  const tooManyPostsUserId = dataResult.records[0].get(0).identity;
  const noCreatorPostId = dataResult.records[0].get(1).identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(1);
  expect(violatingEdges.records[0].get(0).identity).toEqual(noCreatorPostId);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(1);
  expect(violatingEdges.records[0].get(0).identity).toEqual(tooManyPostsUserId);
});
