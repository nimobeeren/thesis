# Typing Property Graphs

[![Build thesis PDF](https://github.com/nimobeeren/thesis/actions/workflows/latex.yml/badge.svg)](https://github.com/nimobeeren/thesis/actions/workflows/latex.yml)

## Dependencies

- Neo4j Community Edition 4.4.7
- APOC 4.4.0.5

## Required Configuration

For security reasons, procedures that use internal Neo4j APIs are disabled by default. They must be enabled in configuration file `$NEO4J_HOME/conf/neo4j.conf` by setting `dbms.security.procedures.unrestricted=apoc.*`.
