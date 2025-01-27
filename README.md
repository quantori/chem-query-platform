# Chemical Query Platform

<hr>
Copyright (c) 2025 Quantori.

Chemical Query Platform (CQP) is an open-source framework designed for indexing and searching within cheminformatics
applications (molecules & reactions).
It supports multiple storage engines, including PostgreSQL, Elasticsearch, and Apache Solr, and is built to address the
growing demand
for efficient data analysis in the biotechnology and healthcare sectors.
Given the continuous growth of data amount appearing due to the ongoing development of omics technologies and
health-related Industry 4.0, CQP can provide efficient scalability and flexibility for multimodal data integration and
search. The main emphasis of CQP is on the chemical structure and reaction search capabilities, while its design enables
application in small-molecule compound and chemical reaction databases, two important domains of cheminformatics.
Powered by the Akka Actors framework, CQP provides an efficient, highly scalable, and flexible environment to run
complex data operations over multiple repositories with various search algorithms.

CQP offers the homogeneous outer interface of several formats of request integrations and performance of asynchronous
Long Running Operations, which simplifies the implementation. Because of its modular architecture, custom search
applications may be developed fast in a pharmaceutical, biotech, and academic research environment to fit specific
needs. It offers the most efficient and powerful solutions both for indexing massive datasets and cross-searches in
biomedical data analysis and integration.

<hr> 

## Chemical Query Platform (CQP) Features:

### Framework for Search Applications:

* Supports various data abstractions (molecules, chemical reactions).
* Utilizes Akka Actors for scalability, clustering, and parallel execution.

### Indexing and Searching:

* Supports chemical structure and reaction search in relational databases.
* Parallel execution of search queries in local or cluster environments.

### Data Storage Integration:

* Organizes implementations for multiple storage types.
* Provides a single external interface for different request formats.
* Allows long-running asynchronous operations on data.
* Cache management and buffering for search queries.

## License

<hr>
Chemical Query Platform is released under [Apache License, Version 2.0](LICENSE)
