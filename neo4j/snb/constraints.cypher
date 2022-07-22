CREATE CONSTRAINT FOR (n:Forum) REQUIRE n.title IS NOT NULL;
CREATE CONSTRAINT FOR (n:Forum) REQUIRE n.creationDate IS NOT NULL;

CREATE CONSTRAINT FOR (n:Message) REQUIRE n.browserUsed IS NOT NULL;
CREATE CONSTRAINT FOR (n:Message) REQUIRE n.creationDate IS NOT NULL;
CREATE CONSTRAINT FOR (n:Message) REQUIRE n.locationIP IS NOT NULL;
CREATE CONSTRAINT FOR (n:Message) REQUIRE n.length IS NOT NULL;

CREATE CONSTRAINT FOR (n:Organisation) REQUIRE n.name IS NOT NULL;
CREATE CONSTRAINT FOR (n:Organisation) REQUIRE n.url IS NOT NULL;

CREATE CONSTRAINT FOR (n:Place) REQUIRE n.name IS NOT NULL;
CREATE CONSTRAINT FOR (n:Place) REQUIRE n.url IS NOT NULL;

CREATE CONSTRAINT FOR (n:Tag) REQUIRE n.name IS NOT NULL;
CREATE CONSTRAINT FOR (n:Tag) REQUIRE n.url IS NOT NULL;

CREATE CONSTRAINT FOR (n:TagClass) REQUIRE n.name IS NOT NULL;
CREATE CONSTRAINT FOR (n:TagClass) REQUIRE n.url IS NOT NULL;

CREATE CONSTRAINT FOR (n:Person) REQUIRE n.firstName IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.lastName IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.gender IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.birthday IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.speaks IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.browserUsed IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.locationIP IS NOT NULL;
CREATE CONSTRAINT FOR (n:Person) REQUIRE n.creationDate IS NOT NULL;

CREATE CONSTRAINT FOR ()-[e:HAS_MEMBER]-() REQUIRE e.creationDate IS NOT NULL;
CREATE CONSTRAINT FOR ()-[e:KNOWS]-() REQUIRE e.creationDate IS NOT NULL;
CREATE CONSTRAINT FOR ()-[e:LIKES]-() REQUIRE e.creationDate IS NOT NULL;
CREATE CONSTRAINT FOR ()-[e:STUDY_AT]-() REQUIRE e.classYear IS NOT NULL;
CREATE CONSTRAINT FOR ()-[e:WORK_AT]-() REQUIRE e.workFrom IS NOT NULL;
