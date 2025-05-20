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

Although the case does not explicitly require a defined data architecture, this section presents how the data would be organized following the Medallion architecture principles. Note that in the provided notebook, all logic is consolidated into a single flow to simplify the evaluation of the implementation. Additionally, the CDI interest rate table is dynamically created at runtime for demonstration purposes.

![alt text](diagrams/data_architecture.png?raw=true "Data Architecture")

**Bronze Layer**

The Bronze layer stores raw, unprocessed data ingested directly from the source systems:

- `raw_cdc_data`: Contains the wallet transactions in a change data capture (CDC) format. This table serves as the basis for building account balances over time.
- `raw_cdi_interest`: Contains the historical daily CDI interest rates, after being obtained via API or another system. No transformation is applied to this layer.

This layer ensures data traceability and allows reprocessing in the event of upstream changes or errors.


**Silver Layer**

The Silver layer applies cleansing, normalization and business modeling to prepare the data for analytical use:

- `wallet_history`: A curated and validated representation of each user's wallet balance by day.

- `cdi_interest_history`: A clean, structured version of CDI interest rates, with consistent formatting and guaranteed integrity over the expected date ranges.

These intermediate tables separate the raw ingestion from the final business logic, allowing for easier testing and troubleshooting.

**Gold Layer**

The Gold layer contains results with the business logic applied by combining the necessary data sources and is also where the final version of the Data Products are made available:

- `cdi_payout`: This table is the final product of the pipeline. It contains the daily CDI bonus payment per account, calculated by joining wallet_history with cdi_interest_history and applying all the eligibility criteria. This dataset is intended to be consumed by downstream systems, such as a production relational database, for further processing and making available to the end user.

This layered approach allows for easy use of the data, providing a clear lineage and a reliable database for extending the pipeline with additional use cases by reusing the data from the Data Product.


After presenting the architecture of the solution and the data, the data product documentation is available here: [Data Product Documentation](https://github.com/gabrielsribe/RecargaPay-CDI-Bonus-Assignment/blob/main/data_product/README.md)

The notebook is available here: [Notebook](https://github.com/gabrielsribe/RecargaPay-CDI-Bonus-Assignment/blob/main/notebook/CDI%20Bonus%20Assignment%20-%20Gabriel%20Ribeiro.ipynb)

The documentation for the intermediate table can be found here [User Balance Table](https://github.com/gabrielsribe/RecargaPay-CDI-Bonus-Assignment/blob/main/user_balance_table/README.md)