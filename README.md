# AstraDB

[![Java
21](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/Build-Maven-C71A36?logo=apachemaven&logoColor=white)](https://maven.apache.org/)
[![Apache
Calcite](https://img.shields.io/badge/SQL_Planning-Apache_Calcite-2E6E9E)](https://calcite.apache.org/)
[![RocksDB](https://img.shields.io/badge/Storage-RocksDB-4B6F9C)](https://rocksdb.org/)
[![Project
Status](https://img.shields.io/badge/Project-In_Development-yellow)](#)

AstraDB is a distributed SQL database built in Java. It brings together
core database components such as SQL planning and execution,
transactional storage, replication, and distributed coordination.

## Core Components

-   **SQL Engine** --- SQL parsing, validation, and logical query
    planning.
-   **Query Executor** --- Physical operators for relational query
    execution.
-   **Storage Engine** --- Persistent, transaction-aware data storage.
-   **Transaction Manager** --- Transaction lifecycle and MVCC-based
    visibility.
-   **Raft Consensus** --- Consensus primitives for distributed
    coordination.
-   **Cluster and Metadata** --- Foundations for cluster management and
    database metadata.

## Build

Build the project and run the tests from the repository root:

``` bash
mvn clean verify
```

## Documentation

Detailed architecture, design decisions, and implementation notes are
maintained separately in the project documentation.

------------------------------------------------------------------------

AstraDB is an evolving project. Features and interfaces may change as
development progresses.
