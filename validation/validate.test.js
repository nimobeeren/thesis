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
    RETURN created
  `);

  const createdEdgeId = dataResult.records[0].get("created").identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  // The CREATED edge is missing the at property
  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(1);
  expect(violatingEdges.records[0].get(0).identity).toEqual(createdEdgeId);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("node is missing incoming edge", async () => {
  // Create the schema
  // Every Post must be created by at least one User
  await session.run(`
    CREATE (:Schema:User { name: "STRING", age: "INTEGER" })
    -[:CREATED {__inMin: 1}]->
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
  // Every User must create at least one Post
  await session.run(`
    CREATE (:Schema:User { name: "STRING", age: "INTEGER" })
    -[:CREATED {__outMin: 1}]->
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

  const userNodeId = dataResult.records[0].get("selfloop").identity;

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

test("multiple cardinality constraints are violated", async () => {
  // Create the schema
  // Users can create 0 or 1 Posts, and every Post is created by exactly 1 User
  await session.run(`
    CREATE (:Schema:User)
    -[:CREATED {__inMin: 1, __inMax: 1, __outMin: 0, __outMax: 1}]->
    (:Schema:Post)
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (:Data:User),
    (:Data:User)-[:CREATED]->(:Data:Post),
    (:Data:Post)<-[:CREATED]-(tooManyPostsUser:Data:User)-[:CREATED]->(:Data:Post),
    (noCreatorPost:Data:Post),
    (:Data:User)-[:CREATED]->(tooManyCreatorsPost:Data:Post)<-[:CREATED]-(:Data:User)
    RETURN tooManyPostsUser, noCreatorPost, tooManyCreatorsPost
  `);

  const tooManyPostsUserId = dataResult.records[0].get(0).identity;
  const noCreatorPostId = dataResult.records[0].get(1).identity;
  const tooManyCreatorsPostId = dataResult.records[0].get(2).identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(2);
  expect(violatingIncomingEdges.records[0].get(0).identity).toEqual(
    noCreatorPostId
  );
  expect(violatingIncomingEdges.records[1].get(0).identity).toEqual(
    tooManyCreatorsPostId
  );

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(1);
  expect(violatingOutgoingEdges.records[0].get(0).identity).toEqual(
    tooManyPostsUserId
  );
});

test("nodes can have optional properties", async () => {
  // Create the schema
  // gender property is optional
  await session.run(`
    CREATE (:Schema:User {name: "STRING", age: "INTEGER", gender: "STRING?"})
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (:Data:User {name: "Rose", age: 24, gender: "F"}),
    (:Data:User {name: "Ymke", age: 22}),
    (notAllowed:Data:User {name: "Francesca", age: 42, goth: true}),
    (wrongType:Data:User {name: "Hank", age: 11, gender: 2}),
    (missingMandatory:Data:User {name: "Nick"})
    RETURN notAllowed, wrongType, missingMandatory
  `);

  const notAllowedUserId = dataResult.records[0].get("notAllowed").identity;
  const wrongTypeUserId = dataResult.records[0].get("wrongType").identity;
  const missingMandatoryUserId =
    dataResult.records[0].get("missingMandatory").identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(3);
  // User has a property not specified in the schema
  expect(violatingNodes.records[0].get(0).identity).toEqual(notAllowedUserId);
  // User has an optional property, but the wrong type
  expect(violatingNodes.records[1].get(0).identity).toEqual(wrongTypeUserId);
  // User is missing a mandatory property
  expect(violatingNodes.records[2].get(0).identity).toEqual(
    missingMandatoryUserId
  );

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(0);

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});

test("edges can have optional properties", async () => {
  // Create the schema
  // reason property is optional
  await session.run(`
    CREATE (:Schema:User)-[:CREATED {at: "STRING", reason: "STRING?"}]->(:Schema:Post)
  `);

  // Create the data
  const dataResult = await session.run(`
    CREATE (:Data:User)-[:CREATED {at: "2022", reason: "bored"}]->(:Data:Post),
    (:Data:User)-[:CREATED {at: "2021"}]->(:Data:Post),
    (:Data:User)-[notAllowed:CREATED {at: "2020", junk: true}]->(:Data:Post),
    (:Data:User)-[wrongType:CREATED {at: "2019", reason: 1}]->(:Data:Post),
    (:Data:User)-[missingMandatory:CREATED]->(:Data:Post)
    RETURN notAllowed, wrongType, missingMandatory
  `);

  const notAllowedUserId = dataResult.records[0].get("notAllowed").identity;
  const wrongTypeUserId = dataResult.records[0].get("wrongType").identity;
  const missingMandatoryUserId =
    dataResult.records[0].get("missingMandatory").identity;

  const violatingNodes = await validateNodes(session);
  expect(violatingNodes.records).toHaveLength(0);

  const violatingEdges = await validateEdges(session);
  expect(violatingEdges.records).toHaveLength(3);
  // CREATED edge has a property not specified in the schema
  expect(violatingEdges.records[0].get(0).identity).toEqual(notAllowedUserId);
  // CREATED edge has an optional property, but the wrong type
  expect(violatingEdges.records[1].get(0).identity).toEqual(wrongTypeUserId);
  // CREATED edge is missing a mandatory property
  expect(violatingEdges.records[2].get(0).identity).toEqual(
    missingMandatoryUserId
  );

  const violatingIncomingEdges = await validateIncomingEdges(session);
  expect(violatingIncomingEdges.records).toHaveLength(0);

  const violatingOutgoingEdges = await validateOutgoingEdges(session);
  expect(violatingOutgoingEdges.records).toHaveLength(0);
});
