#  RecargaPay-CDI-Bonus-Assignment
## Gabriel de Souza Ribeiro
## About
The purpose of this repository is to present the solution implemented for the CDI Bonus Assignment case as part of the hiring process for the data engineering position. 

In addition to implementing the calculation logic and building the tables, this documentation also covers the design of the pipeline and the data architecture. The aim is to present a complete solution that goes beyond basic processing, considering aspects such as scalability, reliability and maintainability.

The description of the data pipeline includes the flow from the ingestion of CDC files to the generation of CDI interest payments, with the intermediate stages of transformation and validation. The data architecture is detailed with a focus on the decisions that guarantee the sustainability of the process, such as the choice of the Medallion standard and integration with transactional database systems.

This approach makes it possible not only to meet the project's immediate requirements, but also to lay the foundations for future developments.

## Data pipeline architecture
### High-level overview

To build a robust, observable, and scalable pipeline for calculating and distributing the CDI Bonus, I designed a data pipeline based on the following high-level architecture:

![alt text](diagrams/overall_architecture.png?raw=true "Architecture")

The pipeline is orchestrated using Apache Airflow, which handles scheduling, dependency management and monitoring. Data is organized using the medallion architecture (Bronze -> Silver -> Gold), separating responsibilities between the ingestion, transformation and business logic layers.

**Why Airflow?**

Airflow was chosen because of its mature ecosystem for data orchestration and native features for:

 - Observability at task level: retrys logic, SLAs, failure notifications.
 - Ease of backfilling and retrys: Important for a pipeline dealing with historical balance calculations.
 - Auditability: Each DAG run can be traced, logged and version-controlled.

**Medallion architecture and Delta Lake**

The solution uses Delta Lake and Delta Tables as the storage format for all data layers. Delta Lake supports ACID transactions, metadata manipulation and time travel features, which are very useful for working with historical records, especially in scenarios involving corrections or audits. Combined with the medallion architecture, Delta Lake enables a Lakehouse architecture, combining the reliability and governance of data warehouses with the flexibility and scalability of data lakes.

About the data layers: 
 - Bronze Layer: Ingests raw CDC (Change Data Capture) files. No transformation is applied, only the raw data is kept for traceability and recoverability. 
 -  Silver Layer: Applies validation, deduplication and modeling of historical wallet transaction data. It guarantees the quality and organization of the data, providing a validated balance per day 
 -  Gold Layer: Applies the business logic for the CDI Bonus. This layer combines the balance eligible for the CDI bonus with the daily interest rates and produces the final result for payment.

**Monitoring and observability**

Given the mission-critical nature of the service, the following monitoring strategies are recommended:

- Integrated Airflow monitoring (e.g. email alerts on failures, SLA miss notifications).
- Customized data quality checks (e.g. null data, missing data). These checks will be executed via a Pyspark processing script, allowing quick action to be taken if necessary.

This solution enables problems to be diagnosed quickly and the process to remain transparent to data engineers and stakeholders.

## Data architecture
High-level view of the data architecture: