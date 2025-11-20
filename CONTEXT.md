# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Chemical Query Platform (CQP)

CQP is an open-source framework for indexing and searching within cheminformatics applications (molecules & reactions). It's built on the Akka Actors framework for scalability and supports multiple storage engines including PostgreSQL, Elasticsearch, and Apache Solr.

## Package Naming Strategy

**CQP Internal Code**: All code in this repository uses the `com.quantori.cqp.*` package hierarchy:
- `com.quantori.cqp.api.model.*` - Core data models (Property, PropertyType, PropertyValue, etc.)
- `com.quantori.cqp.core.*` - Akka actors and core framework
- `com.quantori.cqp.storage.elasticsearch.*` - Elasticsearch storage implementation
- `com.quantori.cqp.build.*` - Gradle build plugins

**External Indigo Library**: The external Indigo Toolkit library uses `com.epam.indigo.*`:
- `com.epam.indigo.Indigo` - Main Indigo class
- `com.epam.indigo.IndigoObject` - Indigo molecule/reaction objects
- `com.epam.indigo.IndigoException` - Indigo exceptions
- **Do not change** these package references - they belong to the external library

## Project Structure

This is a multi-module Gradle project with the following key modules:

- **cqp-api**: Core API and storage abstractions for chemical structures (molecules/reactions)
- **cqp-core**: Akka-based core framework with actors for search, task management, and clustering (currently deleted files in git status)
- **cqp-storage-elasticsearch**: Elasticsearch storage implementation
- **cqp-build**: Custom Gradle build plugins

The project uses a modular architecture where storage implementations (like Elasticsearch) implement the interfaces defined in cqp-api.

## Build Commands

```bash
# Build the entire project
./gradlew build

# Clean build artifacts
./gradlew clean

# Build and publish to local Maven repository
./gradlew build publishToMavenLocal

# Run tests for all modules
./gradlew test

# Run tests for specific module
./gradlew :cqp-api:test
./gradlew :cqp-storage-elasticsearch:test

# Code formatting (Spotless)
./gradlew spotlessApply

# Check code formatting
./gradlew spotlessCheck
```

## Key Technologies

- **Akka Actors**: For distributed computing and actor-based concurrency
- **Indigo Toolkit**: Chemical structure processing and fingerprint calculation
- **Elasticsearch**: Primary storage backend for chemical data
- **Gradle**: Build system with custom plugins
- **JUnit 5**: Testing framework
- **Testcontainers**: Integration testing with containerized dependencies

## Architecture Notes

- The system is designed around chemical structure search capabilities (exact, substructure, similarity)
- Uses fingerprint-based indexing for efficient chemical structure searching
- Supports both molecules and chemical reactions as first-class entities
- Actor-based architecture enables distributed processing and clustering
- Storage layer is abstracted to support multiple backends through common interfaces

## Chemical Data Model

Core entities include:
- **Molecules**: Chemical structures with fingerprints for searching
- **Reactions**: Chemical transformations with participants (reactants, products, catalysts)
- **Libraries**: Collections of molecules or reactions
- **Search Parameters**: Various search types (exact, substructure, similarity) with configurable parameters

## Testing

Tests use JUnit 5 with Mockito for mocking. Integration tests leverage Testcontainers for testing against real Elasticsearch instances.