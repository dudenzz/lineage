# Link Prediction for Data Lineage Discovery

Data lineage describes relationships between data instances that capture data origin and derivation. In relational databases, lineage often manifests as dependencies between tables, where the content of one table is derived from others through queries, transformations, or procedural logic. However, such lineage information is rarely stored explicitly, making automated discovery methods highly valuable.

This repository focuses on **automated data lineage discovery in relational databases** using **knowledge graphs and graph neural networks (GNNs)**. We demonstrate a wide range of lineage scenarios commonly encountered in real-world data systems, including:

- **Basic Data Copy** – Simple data movement using `INSERT … SELECT`
- **Data Transformation and Aggregation** – Lineage involving transformation functions (e.g., `SUM`, `AVG`, `CASE`)
- **Join-based Data Creation** – New datasets created by joining multiple source tables
- **Derived Views** – Lineage inferred through SQL views
- **Temporary Table Utilization** – Intermediate tables used during data creation, potentially obscuring lineage links
- **Stored Procedure Execution** – Lineage resulting from procedural logic that generates or inserts data
- **Recursive Queries and Hierarchical Data** – Data creation using recursive `WITH` Common Table Expressions (CTEs)
- **Data Deduplication and Filtering** – Lineage affected by record elimination or selective inserts
- **Partitioned Data Processing** – Inserts or updates based on partitioning conditions (e.g., time ranges, regions)
- **Materialized Summary Tables** – Pre-aggregated tables created for performance optimization
- **Tabular Function Lineage** – Lineage derived through tabular (table-valued) functions

## Dataset and Knowledge Graph Construction

As a baseline database, we use the widely known open-source **Northwind** database. For each lineage scenario, we generate **five examples** using **Large Language Models (LLMs)** to synthesize realistic SQL transformations and data workflows.

The resulting relational database is converted into a **knowledge graph** using the `owlready2` library and a **custom low-level ontology** designed specifically for relational data lineage representation. The graph captures multiple types of relations, including structural metadata (e.g., tables, columns, rows) alongside lineage relationships.

## Link Prediction with Graph Neural Networks

The constructed knowledge graph is used to train a **Graph Neural Network for inductive link prediction**. Notably, the model **does not distinguish between relation types** during training; lineage relations are learned jointly with other graph relations (e.g., `hasColumn`, `hasRow`).

We base our approach on the GNN architecture described in:

> *“Inductive Link Prediction in Knowledge Graphs using Path-based Neural Networks”*  
> Canlin Zhang, Xiuwen Liu

The original implementation was modified minimally to ensure compatibility with modern library versions and to support our ontology and data model.

## Evaluation

The trained model is evaluated on a test dataset containing only triples with **lineage relations**. Preliminary results show **approximately 90% accuracy**, indicating strong potential for knowledge-graph-based lineage discovery.

---

## Repository Contents

This repository includes:
- LLM prompts
- SQL transformation scripts
- Knowledge graph creation scripts
- A modified GNN implementation for link prediction