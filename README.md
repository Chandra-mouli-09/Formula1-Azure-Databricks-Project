
# Formula1 Data Engineering Project using Azure Databricks, Delta Lake & Unity Catalog

## Overview

This project demonstrates an end-to-end Data Engineering solution built on the Azure Data Platform using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, Unity Catalog, and Lakeflow Jobs.

The solution ingests Formula 1 racing data from the Ergast API, stores it in a scalable data lake, processes it through a Medallion Architecture (Bronze, Silver, Gold), and exposes curated datasets for analytics and reporting.

The project focuses on implementing modern Lakehouse architecture principles, data governance, incremental processing, and workflow orchestration using Databricks.

---

## Project Architecture
architecture/formula1_architecture_diagram.png

### Data Flow

Ergast API
→ Azure Data Factory (ADF)
→ Azure Data Lake Storage Gen2
→ Databricks Ingestion Notebooks
→ Bronze Schema
→ Silver Schema
→ Gold Schema
→ SQL Analytics
→ Analytics Schema
→ Reporting & Insights

## Business Objective

Formula 1 generates large volumes of race-related data, including drivers, constructors, races, circuits, race results, and sprints results.

The objective of this project is to:

* Build an automated and scalable data pipeline.
* Implement a Lakehouse architecture using Delta Lake.
* Enable analytical reporting on race performance.
* Provide governed and discoverable data assets.
* Demonstrate modern Azure Data Engineering practices.
---

## Technology Stack

| Category          | Technology                   |
| ----------------- | ---------------------------- |
| Cloud Platform    | Microsoft Azure              |
| Data Ingestion    | Azure Data Factory           |
| Data Storage      | Azure Data Lake Storage Gen2 |
| Processing Engine | Azure Databricks             |
| Transformation    | PySpark                      |
| Data Format       | Delta Lake                   |
| Governance        | Unity Catalog                |
| Orchestration     | Lakeflow Jobs                |
| Query Engine      | Databricks SQL               |
| Version Control   | GitHub                       |

---

## Dataset

Source: Ergast Formula 1 API

The project processes historical Formula 1 racing data including:

* Circuits
* Constructors
* Drivers
* Races
* Results
* Sprints
---

## Medallion Architecture

### Bronze Layer

Purpose:
Store raw data exactly as received from the source.

Activities:

* Data ingestion from API
* Schema application
* Audit column generation
* Delta table creation

Examples:

* bronze.circuits
* bronze.drivers
* bronze.races
* bronze.results

---

### Silver Layer

Purpose:
Clean, standardize, and validate data.

Activities:

* Data cleansing
* Column standardization
* Null handling
* Data quality validation
* Business rule implementation

Examples:

* silver.circuits
* silver.drivers
* silver.races
* silver.results

---

### Gold Layer

Purpose:
Provide business-ready datasets for reporting and analytics.

Activities:
* Data modeling 
* Aggregations
* Joins
* KPI calculations
* Reporting datasets

Examples:

* gold.dim_drivers
* gold.dim_constructors
* gold.fact_results_session

---

## Unity Catalog Implementation

This project utilizes Unity Catalog for centralized governance.

Features implemented:

* Centralized metadata management
* Fine-grained access control
* Data lineage tracking
* Data discovery
* Auditability

Catalog Structure:

formula1_catalog

├── landing_schema

├── bronze_schema

├── silver_schema

├── gold_schema

└── analytics_schema

---

## Lakeflow Jobs

Lakeflow Jobs are used to automate and orchestrate the end-to-end pipeline.

Features:

* Scheduled execution
* Dependency management
* Monitoring
* Failure handling
* Automated workflow execution

---

## Key Features

✔ End-to-End Data Engineering Pipeline

✔ Azure Data Factory Integration

✔ Azure Data Lake Storage Gen2

✔ Databricks Notebook Development

✔ PySpark Transformations

✔ Delta Lake Implementation

✔ Medallion Architecture

✔ Unity Catalog Governance

✔ Lakeflow Workflow Orchestration

✔ SQL-Based Analytics

✔ Incremental Data Processing

---

## Repository Structure

Formula1-Azure-Databricks-Project/

├── architecture/

├── formula1/
  ├── 1.setup/
  ├── 2.bronze/
  ├── 3.silver/
  ├── 4.gold/
  ├── 5.analytics/
  
└── README.md

---

## Sample Analytics

The Gold Layer enables business analysis such as:

* Driver Championship Standings
* Constructor Championship Standings
* Dominant Drivers all time
* Dominant Constructors all time

---

## Skills Demonstrated

This project demonstrates hands-on experience in:

* Azure Data Engineering
* PySpark Development
* Delta Lake
* Data Lake Architecture
* ETL/ELT Pipelines
* Data Governance
* Workflow Orchestration
* SQL Analytics
* GitHub Version Control

---

## Future Enhancements

* CI/CD using GitHub Actions
* Infrastructure as Code using Terraform
* Data Quality Framework
* Streaming Data Processing
* Power BI Dashboard Integration
* Monitoring & Alerting Framework

---

## Author

Chandra Mouli

Azure Data Engineering | PySpark | Databricks | SQL | Data Analytics

GitHub:
https://github.com/Chandra-mouli-09

LinkedIn:
(Add your LinkedIn profile URL)

---

## Project Outcome

This project successfully demonstrates how modern Azure services can be combined to build a scalable, governed, and production-ready Lakehouse architecture capable of processing Formula 1 racing data for analytical use cases.
