# Chemical Query Platform (CQP) Architecture

## Overview

The Chemical Query Platform (CQP) is an open-source framework for indexing and searching chemical structures (molecules and reactions) in cheminformatics applications. It provides a clean separation between API contracts and storage implementations, enabling multiple backend engines (Elasticsearch, PostgreSQL, Solr) while maintaining a consistent interface.

## Technology Stack

### Core Platform
- **Java 17**: Modern LTS Java with records and pattern matching
- **Gradle 8.x**: Build system with Kotlin DSL
- **Akka Actors 2.9.0**: Distributed computing framework (cqp-core module)

### Chemical Libraries
- **Indigo Toolkit 1.22.0-rc.2**: Structure processing, fingerprinting, matching
- **Object Pooling**: Custom pool management for expensive Indigo instances

### Storage Engines
- **Elasticsearch 8.6.2**: Primary production storage (cqp-storage-elasticsearch)
- **PostgreSQL**: Alternative backend (interface support)
- **Apache Solr**: Alternative backend (interface support)

### Data & Serialization
- **Jackson 2.15.2**: JSON serialization
- **Lombok 1.18.28**: Boilerplate reduction
- **Apache Commons**: Text 1.10.0, Lang3 3.12.0

### Testing
- **JUnit 5.10.3**: Testing framework
- **Mockito 4.11.0/5.12.0**: Mocking
- **Testcontainers 1.17.6**: Integration testing with real Elasticsearch
- **AssertJ 3.21.0**: Fluent assertions
- **Awaitility 4.1.0**: Async testing

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   ChemFlect Backend                         │
│              (Java/Spring Boot Consumer)                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ Uses CQP API
                     ↓
┌─────────────────────────────────────────────────────────────┐
│                      cqp-api                                │
│  Storage Abstractions & Chemical Data Models                │
│  ┌─────────────────────────────────────────────────┐        │
│  │ StorageMolecules Interface                      │        │
│  │ - searchExact(), searchSub(), searchSim()       │        │
│  │                                                 │        │
│  │ StorageReactions Interface                      │        │
│  │ - searchReactions(), searchParticipants()      │        │
│  │                                                 │        │
│  │ StorageLibrary Interface                        │        │
│  │ - createLibrary(), getLibrary(), delete()       │        │
│  └─────────────────────────────────────────────────┘        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ Implemented by
                     ↓
┌─────────────────────────────────────────────────────────────┐
│              cqp-storage-elasticsearch                      │
│  ┌──────────────────────────────────────────────┐           │
│  │ ElasticsearchStorageMolecules                │           │
│  │ - Implements molecule search                 │           │
│  │ - Uses Indigo for fingerprints               │           │
│  │ - Bool queries + script score                │           │
│  │                                               │           │
│  │ ElasticsearchStorageReactions                │           │
│  │ - Implements reaction search                 │           │
│  │ - Manages reactions + participants indices   │           │
│  │                                               │           │
│  │ ElasticsearchStorageLibrary                  │           │
│  │ - Creates/manages indices per library        │           │
│  └──────────────────────────────────────────────┘           │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│                  Elasticsearch 8.6.2                        │
│  Indices:                                                   │
│  - molecules_{libraryId}                                    │
│  - reactions_{libraryId}                                    │
│  - reactions_participants_{libraryId}                       │
│  - cqp_libraries                                            │
└─────────────────────────────────────────────────────────────┘
```

## Module Structure

### cqp-api (Core API Module)

**Version**: 0.0.17

**Purpose**: Defines interfaces, models, and abstractions for chemical structure storage and search.

**Key Packages**:
```
com.quantori.cqp.api/
├── indigo/
│   ├── IndigoProvider.java              # Indigo instance pooling
│   ├── IndigoInchiProvider.java         # InChI instance pooling
│   ├── IndigoFactory.java               # Factory for Indigo creation
│   └── ObjectPool.java                  # Generic object pool interface
├── model/
│   ├── upload/                          # Input models
│   │   ├── Molecule.java                # Molecule with fingerprints
│   │   ├── BasicMolecule.java           # Minimal molecule data
│   │   ├── Reaction.java                # Reaction definition
│   │   ├── ReactionParticipant.java     # Reaction participant
│   │   └── ReactionUploadDocument.java  # Complete reaction upload
│   └── Flattened.java                   # Output models (Molecule, Reaction)
├── DataStorage.java                     # Generic storage interface
├── StorageMolecules.java                # Molecule operations interface
├── StorageReactions.java                # Reaction operations interface
├── StorageLibrary.java                  # Library management interface
├── SearchIterator.java                  # Batched result iteration
├── ItemWriter.java                      # Streaming write interface
├── MoleculesFingerprintCalculator.java  # Fingerprint calculation
└── MoleculesMatcher.java                # Structure matching logic
```

**Core Interfaces**:

1. **DataStorage<U, I>**: Generic storage contract
2. **StorageMolecules**: Molecule-specific operations
3. **StorageReactions**: Reaction-specific operations
4. **StorageLibrary**: Library CRUD operations

**Dependencies**: Indigo, Jackson, Lombok

### cqp-storage-elasticsearch (Elasticsearch Implementation)

**Version**: 0.0.17

**Purpose**: Elasticsearch implementation of CQP storage interfaces.

**Key Components**:
```
com.quantori.cqp.storage.elasticsearch/
├── ElasticsearchStorageMolecules.java     # Molecule search impl
├── ElasticsearchStorageReactions.java     # Reaction search impl
├── ElasticsearchStorageLibrary.java       # Library management impl
├── ElasticsearchIterator.java             # Scroll API iterator
├── ElasticIndexMappingsFactory.java       # Index schema definitions
├── ElasticsearchMoleculeUploader.java     # Batch molecule uploader
├── ElasticsearchReactionUploader.java     # Batch reaction uploader
├── model/
│   ├── MoleculeDocument.java              # Elasticsearch molecule doc
│   ├── ReactionDocument.java              # Elasticsearch reaction doc
│   └── ReactionParticipantDocument.java   # Participant doc
└── mapper/
    ├── MoleculeMapper.java                # Domain ↔ Elasticsearch
    └── ReactionMapper.java                # Domain ↔ Elasticsearch
```

**Dependencies**: cqp-api (0.0.17), Elasticsearch Java Client (8.6.2)

### cqp-core (Akka Actors Module - Currently Missing)

**Expected Purpose**: Akka-based distributed search and task management.

**Expected Components**:
- Search actors for distributed query execution
- Task management actors
- Cluster coordination
- Message-driven architecture

**Note**: Referenced in `settings.gradle.kts` but deleted in working directory.

### cqp-build (Custom Gradle Plugin)

**Purpose**: Standardized build configuration for all CQP modules.

**Configuration Applied**:
- Java 17 language version
- Maven publishing with GPG signing
- Spotless code formatting
- JUnit 5 testing
- Source and Javadoc JAR generation

**Build Tasks**:
- `publishToMavenLocal`: Publish to local Maven repository
- `uploadToMavenCentral`: Create bundle for Maven Central
- `spotlessApply`: Format code
- `spotlessCheck`: Check formatting

## Property Data Types

### Supported Property Types

CQP defines three core property types for molecule properties:

```java
public enum PropertyType {
    STRING,   // Text values, stored as Elasticsearch 'wildcard' type
    DECIMAL,  // Numeric values (double), stored as Elasticsearch 'double' type
    DATE      // Date/datetime values, stored as Elasticsearch 'date' type
}
```

### Type Mapping in Elasticsearch

**Storage Mapping**:
```
PropertyType.STRING  → Elasticsearch 'wildcard' (pattern matching support)
PropertyType.DECIMAL → Elasticsearch 'double'
PropertyType.DATE    → Elasticsearch 'date' (ISO 8601 format)
```

**Elasticsearch Index Configuration**:
```json
{
  "dynamic_templates": [
    {
      "strings": {
        "match_mapping_type": "string",
        "mapping": {"type": "wildcard"}
      }
    },
    {
      "decimals": {
        "match_mapping_type": "double",
        "mapping": {"type": "double"}
      }
    },
    {
      "integers": {
        "match_mapping_type": "long",
        "mapping": {"type": "double"}  // Integers stored as double
      }
    }
  ],
  "properties": {
    "molproperties": {
      "type": "nested"  // Properties stored as nested documents
    }
  }
}
```

### Property Storage Structure

**Nested Property Documents**:
- Each molecule can have multiple custom properties
- Properties stored as nested objects for efficient filtering
- Property values indexed according to their type
- Supports dynamic property addition

**Query Capabilities by Type**:

**STRING Properties**:
- Wildcard pattern matching (`*value*`)
- Exact match
- Case-sensitive comparison
- Operators: EQUALS, CONTAINS, EXISTS

**DECIMAL Properties**:
- Numeric range queries
- Comparison operators: GT, LT, GTE, LTE, EQ, NEQ
- Sorting support
- Statistical aggregations possible

**DATE Properties**:
- ISO 8601 datetime format
- Range queries (before/after/between)
- Operators: GT, LT, GTE, LTE, EQ
- Timezone-aware comparisons

### Type Conversion Notes

**Integer Handling**:
- Integers automatically converted to DECIMAL (double)
- No separate INTEGER type in CQP
- Maintains numeric precision for integer values
- Backend may reference "INTEGER" as deprecated type

**Date Format Support**:
- Multiple date formats supported during parsing
- Stored in normalized ISO 8601 format
- Date parsing disabled by default (`DATE_PARSE_NOT_ALLOWED = true`)

### Limitations

**CQP-Specific**:
- Only three types supported (STRING, DECIMAL, DATE)
- No boolean type (use STRING with "true"/"false")
- No array/list types
- No nested object types beyond properties
- DATE type detection disabled (treats as STRING unless explicitly set)

**Elasticsearch-Specific**:
- Maximum 512 nested property objects per molecule (Elasticsearch limit)
- Wildcard queries on very long strings may be slow
- No full-text search features (no analyzers applied)

## Chemical Data Models

### Input Models (upload package)

#### BasicMolecule
```java
class BasicMolecule {
    String id;              // Unique identifier
    String smiles;          // SMILES notation
    byte[] structure;       // Binary structure (MOL format)
    byte[] exact;           // Exact fingerprint (binary)
    byte[] sub;             // Substructure fingerprint (binary)
}
```

#### Molecule (extends BasicMolecule)
```java
class Molecule extends BasicMolecule {
    byte[] sim;                      // Similarity fingerprint (512-bit)
    String exactHash;                // Hash of exact fingerprint
    Map<String, String> molProperties; // Custom properties
    Long customOrder;                // Custom sorting field
    Instant createdStamp;            // Creation timestamp
    Instant updatedStamp;            // Last update timestamp
}
```

#### Reaction
```java
class Reaction {
    String id;                  // Unique identifier
    String reactionSmiles;      // Reaction SMILES
    String reactionDocumentId;  // Parent document reference
    String source;              // Data source
    String paragraphText;       // Context paragraph
    String amount;              // Quantity information
    byte[] sub;                 // Substructure fingerprint
    Instant createdStamp;
    Instant updatedStamp;
}
```

#### ReactionParticipant
```java
class ReactionParticipant {
    String id;                          // Identifier
    ReactionParticipantRole role;       // Role in reaction
    String smiles;                      // Structure SMILES
    byte[] structure;                   // Binary structure
    byte[] exact;                       // Exact fingerprint
    byte[] sub;                         // Substructure fingerprint
    String reactionDocumentId;          // Parent reaction
}
```

**ReactionParticipantRole Enum**:
```
product      - Substance formed from reaction
reactant     - Initial substance
spectator    - Present as both reactant and product
solvent      - Dissolves solute (spectator type)
catalyst     - Increases reaction rate (spectator type)
any          - Any type
reaction     - The process itself
none         - Not a reaction part
```

### Output Models (Flattened class)

#### Flattened.Molecule
```java
class Molecule extends upload.Molecule implements SearchItem, StorageItem {
    String libraryId;      // Parent library
    String libraryName;    // Library display name
    String storageType;    // Storage backend type
}
```

#### Flattened.Reaction
```java
class Reaction extends upload.Reaction implements SearchItem, StorageItem {
    String libraryId;
    String libraryName;
    String storageType;
}
```

### Library Model

```java
class Library {
    String id;                  // Unique identifier
    String name;                // Display name
    LibraryType type;           // molecules or reactions
    long structuresCount;       // Count of items
    Instant createdStamp;
    Instant updatedStamp;
}

enum LibraryType {
    molecules,
    reactions
}
```

## Search Architecture

### Search Types

```java
enum SearchType {
    exact,          // Exact structure match (fingerprint hash equality)
    substructure,   // Query is part of database structure
    similarity,     // Similarity-based search (Tanimoto coefficient)
    all             // Retrieve all items (empty substructure search)
}
```

### Search Parameters

#### ExactParams
```java
class ExactParams {
    String searchQuery;     // SMILES or structure for exact match
}
```

#### SubstructureParams
```java
class SubstructureParams {
    String searchQuery;     // Query structure (SMILES/MOL)
}
```

#### SimilarityParams
```java
class SimilarityParams {
    Double alpha;           // Similarity metric parameter
    Double beta;            // Similarity metric parameter
    Double minSim;          // Minimum similarity threshold
    Double maxSim;          // Maximum similarity threshold
    SimilarityMetric metric; // Type of similarity calculation
}
```

#### SortParams
```java
class SortParams {
    List<SortSpec> sortList();  // Sort specifications
}

class SortSpec {
    String fieldName;       // Field to sort by
    Order order;            // ASC or DESC
    Type type;              // FLAT or NESTED (for properties)
}
```

### StorageRequest

**Complete Search Request**:
```java
class StorageRequest {
    String storageName;                     // Target storage identifier
    List<String> indexIds;                  // Library IDs to search
    SearchType searchType;                  // exact/substructure/similarity/all
    Map<String, String> searchProperties;   // Legacy property filters
    ExactParams exactParams;                // Exact search config
    SubstructureParams substructureParams;  // Substructure config
    SimilarityParams similarityParams;      // Similarity config
    ReactionParticipantRole role;           // Reaction participant filter
    List<Property> properties;              // Property filters
    SortParams sortParams;                  // Sort configuration
    Criteria criteria;                      // Advanced filtering
}
```

### Filtering with Criteria

**Criteria Interface**: Marker interface for filter abstractions.

**FieldCriteria**:
```java
class FieldCriteria implements Criteria {
    String fieldName;           // Property name
    Operator operator;          // Comparison operator
    Object value;               // Comparison value
}

enum Operator {
    GT, LT, GTE, LTE, EQ, NEQ,  // Numeric comparisons
    CONTAINS,                    // Text contains
    EXISTS                       // Field presence
}
```

**ConjunctionCriteria**:
```java
class ConjunctionCriteria implements Criteria {
    List<Criteria> criteriaList;    // List of criteria
    Operator operator;              // AND or OR
}

enum Operator {
    AND,    // All criteria must match
    OR      // At least one must match
}
```

**Example Usage**:
```java
// Find molecules with MW > 300 AND LogP < 5
Criteria criteria = new ConjunctionCriteria(
    List.of(
        new FieldCriteria("MW", Operator.GT, 300),
        new FieldCriteria("LogP", Operator.LT, 5)
    ),
    Operator.AND
);
```

## Storage Abstraction Layer

### Core Storage Interfaces

#### DataStorage<U, I>
```java
interface DataStorage<U extends StorageUploadItem, I extends StorageItem> {
    ItemWriter<U> itemWriter(String libraryId);
    List<SearchIterator<I>> searchIterator(StorageRequest request);
}
```

#### StorageMolecules
```java
interface StorageMolecules extends DataStorage<Molecule, Flattened.Molecule> {
    // Fingerprint calculation
    MoleculesFingerprintCalculator fingerPrintCalculator();

    // Search operations
    List<SearchIterator<Flattened.Molecule>> searchExact(StorageRequest request);
    List<SearchIterator<Flattened.Molecule>> searchSub(StorageRequest request);
    List<SearchIterator<Flattened.Molecule>> searchSim(StorageRequest request);
    List<SearchIterator<Flattened.Molecule>> searchAll(StorageRequest request);

    // CRUD operations
    List<Flattened.Molecule> findById(String storageName, List<String> libraryIds, List<String> ids);
    void updateMolecules(String storageName, String libraryId, List<Molecule> molecules);
    void deleteMolecules(String storageName, String libraryId, List<String> moleculeIds);
    long countElements(String storageName, String libraryId);
}
```

#### StorageReactions
```java
interface StorageReactions extends DataStorage<ReactionUploadDocument, Flattened.Reaction> {
    // Fingerprint calculation
    ReactionsFingerprintCalculator fingerPrintCalculator();

    // Search operations
    List<SearchIterator<Flattened.Reaction>> searchReactions(StorageRequest request);
    List<SearchIterator<Flattened.ReactionParticipant>> searchExact(StorageRequest request);
    List<SearchIterator<Flattened.ReactionParticipant>> searchSub(StorageRequest request);
    Map<String, List<Flattened.ReactionParticipant>> searchParticipantsByReactionId(
        String storageName, List<String> libraryIds, Set<String> reactionIds
    );

    // CRUD operations
    List<Flattened.Reaction> findById(String storageName, List<String> libraryIds, List<String> ids);
    void deleteReactions(String storageName, String libraryId, List<String> reactionIds);
    long countElements(String storageName, String libraryId);
}
```

#### StorageLibrary
```java
interface StorageLibrary {
    // Library CRUD
    Library getLibraryById(String storageName, String libraryId);
    Library getLibraryByName(String storageName, String libraryName);
    List<Library> getLibraryByType(String storageName, LibraryType type);
    Library createLibrary(String storageName, Library library);
    Library updateLibrary(String storageName, Library library);
    void deleteLibrary(String storageName, String libraryId);

    // Property schema management
    Map<String, PropertyType> getPropertiesMapping(String storageName, String libraryId);
    void addPropertiesMapping(String storageName, String libraryId, Map<String, PropertyType> properties);
    void updatePropertiesMapping(String storageName, String libraryId, Map<String, PropertyType> properties);
}
```

### Iterator Pattern

**SearchIterator<R>**: Batched result iteration.
```java
interface SearchIterator<R> {
    List<R> next();             // Return batch (empty = end)
    void close();               // Cleanup resources
    String getStorageName();    // Storage identifier
    List<String> getLibraryIds(); // Searched libraries
}
```

**Usage Pattern**:
```java
List<SearchIterator<Flattened.Molecule>> iterators =
    storage.searchExact(request);

for (SearchIterator<Flattened.Molecule> iterator : iterators) {
    try {
        while (true) {
            List<Flattened.Molecule> batch = iterator.next();
            if (batch.isEmpty()) break;

            // Process batch
            for (Flattened.Molecule molecule : batch) {
                // ...
            }
        }
    } finally {
        iterator.close();
    }
}
```

**ItemWriter<T>**: Streaming write interface.
```java
interface ItemWriter<T> {
    void write(T item);     // Write single item
    void flush();           // Flush buffer
    void close();           // Persist and cleanup
}
```

**Usage Pattern**:
```java
try (ItemWriter<Molecule> writer = storage.itemWriter(libraryId)) {
    for (Molecule molecule : molecules) {
        writer.write(molecule);
    }
    writer.flush();
} // Auto-close persists data
```

## Fingerprint System

### Fingerprint Types

1. **Exact Fingerprint**: Unique structure identifier
   - Hash used for exact matching
   - Binary representation

2. **Substructure Fingerprint**: For substructure searches
   - Set bits indicate presence of structural features
   - Query fingerprint must be subset of target

3. **Similarity Fingerprint**: For similarity searches
   - 512-bit fingerprint
   - Tanimoto coefficient calculation

### Fingerprint Calculators

#### MoleculesFingerprintCalculator
```java
interface MoleculesFingerprintCalculator extends BaseFingerprintCalculator {
    byte[] exactFingerprint(String structure);
    byte[] exactFingerprint(byte[] structure);
    byte[] substructureFingerprint(String structure);
    byte[] substructureFingerprint(byte[] structure);
    byte[] similarityFingerprint(String structure);
    byte[] similarityFingerprint(byte[] structure);
}
```

#### ReactionsFingerprintCalculator
```java
interface ReactionsFingerprintCalculator extends BaseFingerprintCalculator {
    byte[] exactFingerprint(String structure);
    byte[] exactFingerprint(byte[] structure);
    byte[] substructureFingerprint(String structure);
    byte[] substructureFingerprint(byte[] structure);
    byte[] substructureReactionFingerprint(String reactionSmiles);
}
```

### IndigoFingerprintCalculator Implementation

**Object Pooling Pattern**:
```java
class IndigoFingerprintCalculator {
    private final ObjectPool<Indigo> indigoPool;

    byte[] exactFingerprint(String structure) {
        Indigo indigo = indigoPool.borrow();
        try {
            IndigoObject mol = indigo.loadMolecule(structure);
            mol.aromatize();
            IndigoObject fingerprint = mol.fingerprint("full");
            return fingerprint.toBuffer();
        } finally {
            indigoPool.returnObject(indigo);
        }
    }
}
```

**Fingerprint Calculation Flow**:
```
Input Structure (SMILES/MOL)
    ↓
Borrow Indigo instance from pool
    ↓
Load molecule
    ↓
Aromatize structure
    ↓
Calculate fingerprint (type-specific)
    ├─→ "full" for exact
    ├─→ "sub" for substructure
    └─→ "sim" for similarity
    ↓
Convert to byte buffer
    ↓
Return Indigo to pool
    ↓
Return byte[]
```

### Fingerprint Utilities

```java
class FingerPrintUtilities {
    static String exactHash(byte[] exact);
    static String substructureHash(byte[] sub);
    static byte[] getSimilarityFingerprint512(byte[] raw);
    static int numberOfNonZeroElements(byte[] fingerprint);
    static byte[] decodeStructure(String encoded);
}
```

### Matchers

**MoleculesMatcher**:
```java
interface MoleculesMatcher {
    boolean isExactMatch(byte[] structure, ExactParams params);
    boolean isSubstructureMatch(byte[] structure, SubstructureParams params);
}
```

**ReactionsMatcher**:
```java
interface ReactionsMatcher {
    boolean isExactMatch(byte[] structure, ExactParams params);
    boolean isSubstructureMatch(byte[] structure, SubstructureParams params);
    boolean isReactionSubstructureMatch(String reactionSmiles, SubstructureParams params);
}
```

**Purpose**: Post-filtering after database search to validate matches.

## Elasticsearch Implementation

### Index Strategy

**One Index Per Library**:
- Molecules: `molecules_{libraryId}`
- Reactions: `reactions_{libraryId}`
- Reaction Participants: `reactions_participants_{libraryId}`
- Library Metadata: `cqp_libraries`

**Benefits**:
- Independent scaling per library
- Isolated deletion
- Custom property schemas per library

### Molecule Search Flow

#### Exact Search
```
Calculate exact fingerprint from query
    ↓
Calculate fingerprint hash
    ↓
Build Elasticsearch query:
  - Term query on exactHash field
  - Library ID filter
  - Property/criteria filters
    ↓
Execute search
    ↓
Return results (no post-filtering needed)
```

**Elasticsearch Query**:
```json
{
  "query": {
    "bool": {
      "must": [
        {"term": {"exactHash": "<hash>"}}
      ],
      "filter": [
        {"terms": {"libraryId": ["lib1", "lib2"]}}
      ]
    }
  }
}
```

#### Substructure Search
```
Calculate substructure fingerprint from query
    ↓
Build Elasticsearch query:
  - Bool query with fingerprint filter
  - Library ID filter
  - Property/criteria filters
    ↓
Execute search with scroll API
    ↓
Post-filter results using MoleculesMatcher
  (validate actual substructure match)
    ↓
Return validated results
```

**Elasticsearch Query**:
```json
{
  "query": {
    "bool": {
      "must": [
        {"match_all": {}}
      ],
      "filter": [
        {"term": {"sub": "<base64_fingerprint>"}},
        {"terms": {"libraryId": ["lib1", "lib2"]}}
      ]
    }
  }
}
```

**Post-Filtering**:
```java
List<Flattened.Molecule> results = new ArrayList<>();
for (MoleculeDocument doc : esResults) {
    if (matcher.isSubstructureMatch(doc.structure, params)) {
        results.add(mapper.toFlattened(doc));
    }
}
```

#### Similarity Search
```
Calculate similarity fingerprint from query
    ↓
Build Elasticsearch query:
  - Script score query with similarity calculation
  - Min/max similarity bounds
  - Library ID filter
    ↓
Execute search
    ↓
Return results sorted by similarity
```

**Elasticsearch Query**:
```json
{
  "query": {
    "script_score": {
      "query": {
        "bool": {
          "filter": [
            {"terms": {"libraryId": ["lib1", "lib2"]}}
          ]
        }
      },
      "script": {
        "source": "cosineSimilarity(params.query_vector, 'sim_vector') + 1.0",
        "params": {
          "query_vector": [/* 512-dim vector */]
        }
      },
      "min_score": 1.5  // minSim threshold
    }
  }
}
```

### Reaction Search Flow

**Two-Index Strategy**:
1. Search `reactions_participants_{libraryId}` for participants matching query
2. Extract reaction IDs from participants
3. Fetch full reactions from `reactions_{libraryId}` by IDs
4. Apply post-filtering

**Participant Role Filtering**:
```java
BoolQuery.Builder query = new BoolQuery.Builder();

if (role == ReactionParticipantRole.spectator) {
    query.should(s -> s.term(t -> t.field("role").value("solvent")));
    query.should(s -> s.term(t -> t.field("role").value("catalyst")));
} else if (role != ReactionParticipantRole.any) {
    query.must(m -> m.term(t -> t.field("role").value(role.name())));
}
```

### Index Mappings

**Molecule Index Mapping**:
```json
{
  "properties": {
    "id": {"type": "keyword"},
    "smiles": {"type": "text"},
    "structure": {"type": "binary"},
    "exact": {"type": "binary"},
    "sub": {"type": "binary"},
    "sim": {"type": "dense_vector", "dims": 512},
    "exactHash": {"type": "keyword"},
    "libraryId": {"type": "keyword"},
    "mol_properties": {
      "type": "nested",
      "properties": {
        "key": {"type": "keyword"},
        "value_string": {"type": "text"},
        "value_decimal": {"type": "double"},
        "value_date": {"type": "date"}
      }
    },
    "createdStamp": {"type": "date"},
    "updatedStamp": {"type": "date"}
  }
}
```

**Reaction Index Mapping**:
```json
{
  "properties": {
    "id": {"type": "keyword"},
    "reactionSmiles": {"type": "text"},
    "reactionDocumentId": {"type": "keyword"},
    "source": {"type": "text"},
    "paragraphText": {"type": "text"},
    "amount": {"type": "text"},
    "sub": {"type": "binary"},
    "libraryId": {"type": "keyword"},
    "createdStamp": {"type": "date"},
    "updatedStamp": {"type": "date"}
  }
}
```

**Reaction Participant Mapping**:
```json
{
  "properties": {
    "id": {"type": "keyword"},
    "role": {"type": "keyword"},
    "smiles": {"type": "text"},
    "structure": {"type": "binary"},
    "exact": {"type": "binary"},
    "sub": {"type": "binary"},
    "reactionDocumentId": {"type": "keyword"},
    "libraryId": {"type": "keyword"}
  }
}
```

### Query Building

**Bool Query Pattern**:
```java
BoolQuery.Builder boolQuery = new BoolQuery.Builder();

// Must clauses (all must match)
boolQuery.must(m -> m.match(ma -> ma.field("field").query("value")));

// Should clauses (at least one matches)
boolQuery.should(s -> s.term(t -> t.field("field").value("value")));

// Filter clauses (must match, no scoring)
boolQuery.filter(f -> f.terms(t -> t.field("libraryId").terms(ids)));

// Must not clauses (must not match)
boolQuery.mustNot(mn -> mn.exists(e -> e.field("field")));
```

**Property Filters**:
```java
// Nested query for molecule properties
NestedQuery nestedQuery = NestedQuery.of(n -> n
    .path("mol_properties")
    .query(q -> q.bool(b -> b
        .must(m -> m.term(t -> t
            .field("mol_properties.key")
            .value("MW")))
        .must(m -> m.range(r -> r
            .field("mol_properties.value_decimal")
            .gt(JsonData.of(300))))
    ))
);
```

**Criteria Conversion**:
```java
Query buildQuery(Criteria criteria) {
    if (criteria instanceof FieldCriteria fc) {
        return switch (fc.operator) {
            case GT -> RangeQuery.of(r -> r.field(fc.fieldName).gt(fc.value));
            case LT -> RangeQuery.of(r -> r.field(fc.fieldName).lt(fc.value));
            case EQ -> TermQuery.of(t -> t.field(fc.fieldName).value(fc.value));
            case CONTAINS -> WildcardQuery.of(w -> w.field(fc.fieldName).value("*" + fc.value + "*"));
            // ...
        };
    } else if (criteria instanceof ConjunctionCriteria cc) {
        BoolQuery.Builder bool = new BoolQuery.Builder();
        for (Criteria c : cc.criteriaList) {
            Query q = buildQuery(c);
            if (cc.operator == AND) bool.must(q);
            else bool.should(q);
        }
        return bool.build().toQuery();
    }
}
```

### Uploaders

**ElasticsearchMoleculeUploader**:
```java
class ElasticsearchMoleculeUploader implements ItemWriter<Molecule> {
    private List<Molecule> batch = new ArrayList<>();
    private static final int BATCH_SIZE = 100;

    public void write(Molecule molecule) {
        // Calculate fingerprints if not provided
        if (molecule.exact == null) {
            molecule.exact = calculator.exactFingerprint(molecule.smiles);
        }
        if (molecule.sub == null) {
            molecule.sub = calculator.substructureFingerprint(molecule.smiles);
        }
        if (molecule.sim == null) {
            molecule.sim = calculator.similarityFingerprint(molecule.smiles);
        }
        molecule.exactHash = FingerPrintUtilities.exactHash(molecule.exact);

        batch.add(molecule);

        if (batch.size() >= BATCH_SIZE) {
            flush();
        }
    }

    public void flush() {
        if (batch.isEmpty()) return;

        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        for (Molecule mol : batch) {
            MoleculeDocument doc = mapper.toDocument(mol);
            bulkBuilder.operations(op -> op
                .index(idx -> idx
                    .index("molecules_" + libraryId)
                    .id(mol.id)
                    .document(doc)
                )
            );
        }

        client.bulk(bulkBuilder.build());
        batch.clear();
    }

    public void close() {
        flush();
        // Update library count
        updateLibraryCount(libraryId);
    }
}
```

### Iterators

**ElasticsearchIterator.Molecules**:
```java
class Molecules extends ElasticsearchIterator<MoleculeDocument, Flattened.Molecule> {
    @Override
    protected Flattened.Molecule map(Hit<MoleculeDocument> hit) {
        MoleculeDocument doc = hit.source();
        return Flattened.Molecule.builder()
            .id(doc.id)
            .smiles(doc.smiles)
            .structure(doc.structure)
            .exact(doc.exact)
            .sub(doc.sub)
            .sim(doc.sim)
            .molProperties(doc.molProperties)
            .libraryId(doc.libraryId)
            .libraryName(getLibraryName(doc.libraryId))
            .storageType("elasticsearch")
            .build();
    }
}
```

**Scroll API Usage**:
```java
// Initial search with scroll
SearchResponse<T> response = client.search(s -> s
    .index(index)
    .query(query)
    .scroll(Time.of(t -> t.time("5m")))
    .size(100)
, docClass);

String scrollId = response.scrollId();
List<T> batch = extractHits(response);

// Continue scrolling
while (!batch.isEmpty()) {
    // Process batch

    // Next scroll
    ScrollResponse<T> scrollResponse = client.scroll(s -> s
        .scrollId(scrollId)
        .scroll(Time.of(t -> t.time("5m")))
    , docClass);

    scrollId = scrollResponse.scrollId();
    batch = extractHits(scrollResponse);
}

// Clear scroll
client.clearScroll(c -> c.scrollId(scrollId));
```

## Build System

### Gradle Configuration

**settings.gradle.kts**:
```kotlin
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

rootProject.name = "chem-query-platform"

include("cqp-core")
include("cqp-api")
include("cqp-storage-elasticsearch")
includeBuild("cqp-build")  // Custom plugin
```

### Version Catalog (gradle/libs.versions.toml)

```toml
[versions]
indigo = "1.22.0-rc.2"
jackson = "2.15.2"
lombok = "1.18.28"
elasticsearch = "8.6.2"
junit = "5.10.3"
mockito = "5.12.0"
testcontainers = "1.17.6"

[libraries]
indigo = { module = "com.epam.indigo:indigo", version.ref = "indigo" }
jackson-databind = { module = "com.fasterxml.jackson.core:jackson-databind", version.ref = "jackson" }
lombok = { module = "org.projectlombok:lombok", version.ref = "lombok" }
elasticsearch-java = { module = "co.elastic.clients:elasticsearch-java", version.ref = "elasticsearch" }
junit-jupiter = { module = "org.junit.jupiter:junit-jupiter", version.ref = "junit" }
mockito-core = { module = "org.mockito:mockito-core", version.ref = "mockito" }
testcontainers-elasticsearch = { module = "org.testcontainers:elasticsearch", version.ref = "testcontainers" }
```

### Custom Build Plugin

**CqpJavaLibraryPlugin.kt**:
```kotlin
class CqpJavaLibraryPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.plugins.apply("java-library")
        project.plugins.apply("maven-publish")
        project.plugins.apply("signing")
        project.plugins.apply("com.diffplug.spotless")

        project.java {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(17))
            }
        }

        project.tasks.withType<Test> {
            useJUnitPlatform()
        }

        project.publishing {
            publications {
                create<MavenPublication>("mavenJava") {
                    from(project.components["java"])
                    artifact(project.tasks.named("sourcesJar"))
                    artifact(project.tasks.named("javadocJar"))
                }
            }
        }
    }
}
```

## Testing

### Test Structure

**Unit Tests** (Fast, no external dependencies):
- `ProcessStructureTest`: Structure processing validation
- `IndigoProviderConcurrencyTest`: Thread-safety verification
- `TestIndigoFingerPrintUtilities`: Fingerprint utilities

**Integration Tests** (Testcontainers for Elasticsearch):
- `ElasticsearchStorageMoleculesIT`: Molecule search integration
- `ElasticsearchStorageReactionsIT`: Reaction search integration
- `ElasticsearchStorageLibraryIT`: Library management

### Test Patterns

**Testcontainers Setup**:
```java
@Testcontainers
class ElasticsearchStorageIT {
    @Container
    static ElasticsearchContainer elasticsearch =
        new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.6.2")
            .withEnv("xpack.security.enabled", "false");

    static ElasticsearchStorageMolecules storage;

    @BeforeAll
    static void setup() {
        String host = elasticsearch.getHttpHostAddress();
        storage = new ElasticsearchStorageMolecules(host);
    }

    @Test
    void testExactSearch() {
        // Create library
        Library library = storage.createLibrary("test", "molecules");

        // Upload molecules
        try (ItemWriter<Molecule> writer = storage.itemWriter(library.id)) {
            writer.write(createTestMolecule("CC(C)C", "1"));
            writer.write(createTestMolecule("CCO", "2"));
        }

        // Search
        StorageRequest request = StorageRequest.builder()
            .storageName("test")
            .indexIds(List.of(library.id))
            .searchType(SearchType.exact)
            .exactParams(new ExactParams("CC(C)C"))
            .build();

        List<SearchIterator<Flattened.Molecule>> iterators =
            storage.searchExact(request);

        List<Flattened.Molecule> results = new ArrayList<>();
        for (SearchIterator<Flattened.Molecule> iter : iterators) {
            while (true) {
                List<Flattened.Molecule> batch = iter.next();
                if (batch.isEmpty()) break;
                results.addAll(batch);
            }
            iter.close();
        }

        assertEquals(1, results.size());
        assertEquals("1", results.get(0).id);
    }
}
```

**Mocking with Mockito**:
```java
@ExtendWith(MockitoExtension.class)
class StorageMoleculesTest {
    @Mock
    private MoleculesFingerprintCalculator calculator;

    @Mock
    private MoleculesMatcher matcher;

    @InjectMocks
    private ElasticsearchStorageMolecules storage;

    @Test
    void testSubstructureSearch() {
        byte[] fingerprint = new byte[]{1, 2, 3};
        when(calculator.substructureFingerprint("CCO"))
            .thenReturn(fingerprint);

        when(matcher.isSubstructureMatch(any(), any()))
            .thenReturn(true);

        StorageRequest request = /* ... */;
        List<SearchIterator<Flattened.Molecule>> results =
            storage.searchSub(request);

        assertFalse(results.isEmpty());
        verify(calculator).substructureFingerprint("CCO");
    }
}
```

## Performance Considerations

### Object Pooling

**Indigo Instance Pooling**:
- Indigo instances are expensive to create (~50ms)
- Pool maintains 10-100 instances
- Timeout prevents indefinite blocking
- Thread-safe borrow/return pattern

**Pool Configuration**:
```java
ObjectPool<Indigo> pool = new IndigoPool(
    maxSize: 50,
    timeout: 30_000  // 30 seconds
);
```

### Batching

**Write Batching**:
- Default batch size: 100 items
- Reduces network overhead
- Bulk API for Elasticsearch

**Read Batching**:
- Scroll API with 100 items per batch
- Prevents large result sets in memory
- Streaming processing

### Index Optimization

**Sharding Strategy**:
- Configure shards based on library size
- Small libraries: 1 shard
- Large libraries: 5+ shards

**Refresh Interval**:
- Default: 1 second
- Can increase for bulk imports
- Trade-off: latency vs throughput

## Future Enhancements

1. **Akka Integration**: Restore cqp-core module for distributed search
2. **Caching**: Redis for frequent search results
3. **Parallel Search**: Multi-threaded search across libraries
4. **Query Optimization**: Query plan analysis and optimization
5. **Additional Storage Backends**: PostgreSQL, Solr implementations
6. **Machine Learning**: Integrate ML models for property prediction
7. **Graph Search**: Reaction pathway search
8. **3D Structure**: Support for conformers and 3D search

## Key Metrics

- **Modules**: 4 (cqp-api, cqp-storage-elasticsearch, cqp-core, cqp-build)
- **Java Version**: 17
- **Elasticsearch**: 8.6.2
- **Indigo**: 1.22.0-rc.2
- **Fingerprint Types**: 3 (exact, substructure, similarity)
- **Search Types**: 4 (exact, substructure, similarity, all)
- **Storage Interfaces**: 3 (molecules, reactions, library)
- **Test Framework**: JUnit 5 with Testcontainers
- **Build System**: Gradle with Kotlin DSL
