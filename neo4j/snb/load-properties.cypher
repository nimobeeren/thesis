// These queries load the multi-valued properties from their separate CSV files and sets them as a property
// with an array value

LOAD CSV WITH HEADERS FROM "file:///dynamic/person_email_emailaddress_0_0.csv" AS line FIELDTERMINATOR "|"
WITH line["Person.id"] AS personId, collect(line.email) AS emails
MATCH (person:Person) WHERE toString(person.id) = personId SET person.email = emails
RETURN count(person);

LOAD CSV WITH HEADERS FROM "file:///dynamic/person_speaks_language_0_0.csv" AS line FIELDTERMINATOR "|"
WITH line["Person.id"] AS personId, collect(line.language) AS languages
MATCH (person:Person) WHERE toString(person.id) = personId SET person.speaks = languages
RETURN count(person);
