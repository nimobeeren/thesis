# Property Graph Schemas

[![Build thesis PDF](https://github.com/nimobeeren/thesis/actions/workflows/latex.yml/badge.svg)](https://github.com/nimobeeren/thesis/actions/workflows/latex.yml)

This repository contains the source code and result data of my master thesis: _Formal Specification and Practical Validation of Property Graph Schemas_.

## Abstract

> Graph databases are increasingly receiving attention from industry and academia, due in part to their flexibility; a schema is often not required. In particular, the _property graph_ model enables natural expression of data from a wide variety of domains. However, schemas can significantly benefit query optimization, data integrity, and documentation. We present a formal property graph schema model based on conceptual data modeling methods, integrating constraints on mandatory and allowed properties, property data types, edge endpoints, and edge cardinality. Moreover, we specify schema validation semantics using first-order logic rules. These rules are implemented using graph queries for _Neo4j_, _JanusGraph_, and _TigerGraph_, which we evaluate through a controlled experiment. Our results demonstrate feasibility of our approach, with execution times scaling linearly with the size of the data.

[**📄 Full PDF**](docs/thesis.pdf)

## Repository Structure

**📁 `analysis`:** Data of experiment results and notebook used to generate plots and statistical analyses.

**📁 `docs`:** LaTeX source code and output for the main thesis document.

**📁 `janusgraph`:** Java source code and utilities for JanusGraph experiments.

**📁 `neo4j`:** Cypher queries and utilities for Neo4j experiments.

**📁 `tigergraph`:** GSQL queries and statements and utilities for TigerGraph experiments.
