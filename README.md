# evaluation-project-retail

# Retail Sales Data Warehouse — End-to-End ETL Pipeline

A production-grade, end-to-end Retail Sales Data Warehouse built on **Databricks**, **AWS S3**, **PySpark**, and **SQL**, following the **Medallion Architecture** pattern. The pipeline ingests raw retail source files, applies data quality rules, models a Star Schema warehouse, and exports analytics-ready datasets.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Problem](#business-problem)
- [Technologies Used](#technologies-used)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Input & Output Files](#input--output-files)
- [Notebook Breakdown](#notebook-breakdown)
  - [00\_start — File Ingestion](#00_start--file-ingestion)
  - [Bronze Layer — Raw Ingestion](#bronze-layer--raw-ingestion)
  - [Silver Layer — Data Cleaning](#silver-layer--data-cleaning)
  - [Gold Layer — Business Warehouse](#gold-layer--business-warehouse)
  - [00\_end — Export Process](#00_end--export-process)
- [Validation Checks](#validation-checks)
- [SCD Type 2 — Historical Tracking](#scd-type-2--historical-tracking)
- [Schema Design](#schema-design)
- [Job Orchestration](#job-orchestration)
- [Advantages](#advantages)

---

## Project Overview

This project implements a complete modern data engineering pipeline that covers:

- File ingestion from AWS S3
- Multi-layer data transformation using Medallion Architecture
- Data quality validation at every stage
- Historical change tracking using SCD Type 2
- Star Schema warehouse modelling
- Automated CSV export of final warehouse tables

---

## Business Problem

Retail companies receive data from multiple operational systems — customers, products, stores, and sales transactions. This raw data often contains:

- Duplicate records
- Invalid or missing values
- Inconsistent formatting
- Referential integrity violations

This pipeline addresses all of these by cleaning the data, validating quality at each layer, maintaining a full history of changes, and delivering reporting-ready warehouse tables.

---

## Technologies Used

| Tool | Purpose |
|---|---|
| Databricks | ETL processing and notebook orchestration |
| AWS S3 | Cloud data storage (source, raw, archive, output) |
| PySpark | File handling and ETL logic |
| SQL (Databricks SQL) | Layer transformations and warehouse modelling |
| Delta Tables | Optimized, ACID-compliant warehouse tables |
| Databricks Workflows | Automated job orchestration |
| GitHub | Version control |

---

## Architecture

This project uses **Medallion Architecture** — a layered approach to organizing data quality and transformations inside Databricks.

```
SFTP Source Files
       │
       ▼
   AWS S3 (sftp/)
       │
       ▼
 ┌─────────────┐
 │   BRONZE    │  Raw ingestion — no transformations
 └─────────────┘
       │
       ▼
 ┌─────────────┐
 │   SILVER    │  Cleaned, validated, standardised data
 └─────────────┘
       │
       ▼
 ┌─────────────┐
 │    GOLD     │  Business-ready Star Schema warehouse
 └─────────────┘
       │
       ▼
 AWS S3 (sftp_out/) — Exported CSVs
```

### Layer Responsibilities

| Layer | Purpose |
|---|---|
| Bronze | Store raw data exactly as received — no business rules applied |
| Silver | Clean, deduplicate, validate, and standardise all entities |
| Gold | Build dimension and fact tables, apply SCD Type 2, serve analytics |

---

## Data Flow

```
SFTP Files → AWS S3 → Bronze → Silver → Gold → Export CSV
```

1. Source files land in `s3://retail-etl-dwh-lakehouse/sftp/`
2. `00_start` detects and archives the latest files, copying them to the raw zone
3. Bronze notebooks ingest raw CSVs into Delta tables without transformation
4. Silver notebooks clean, validate, and standardise all entities
5. Gold notebooks build dimension and fact tables with SCD Type 2 logic
6. `00_end` exports final warehouse tables as CSVs to `sftp_out/`

---

## Input & Output Files

### Input (Source Files)

Placed in: `s3://retail-etl-dwh-lakehouse/sftp/`

| File | Description |
|---|---|
| `customers_src_<timestamp>.csv` | Customer master data |
| `products_src_<timestamp>.csv` | Product catalogue |
| `stores_src_<timestamp>.csv` | Store information |
| `sales_transactions_src_<timestamp>.csv` | Sales transaction records |

Files follow the naming convention `<table>_src_<DDMMYYYYHHMMSS>.csv` to support multi-version detection and latest-file selection.

### Output (Warehouse Exports)

Exported to: `s3://retail-etl-dwh-lakehouse/sftp_out/`

| File | Description |
|---|---|
| `dim_customer.csv` | Customer dimension with SCD Type 2 history |
| `dim_product.csv` | Product dimension |
| `dim_store.csv` | Store dimension |
| `fact_sales.csv` | Sales fact table |

---

## Notebook Breakdown

### 00_start — File Ingestion

Handles file detection, latest-version selection, raw loading, and archiving.

**Key logic:**

```python
from datetime import datetime
import re
from collections import defaultdict
```

| Component | Purpose |
|---|---|
| `datetime` | Generates today's date for dynamic folder paths |
| `re` | Regex-based extraction of table name and timestamp from filenames |
| `defaultdict` | Groups multiple file versions by table name |

**Path structure:**

| Path | Purpose |
|---|---|
| `sftp/` | Incoming source files |
| `raw/` | Latest file ready for Bronze ingestion |
| `archive/<DDMMYYYY>/` | Daily backup for audit and recovery |

**Processing steps:**

1. Read all files from the SFTP S3 path using `dbutils.fs.ls()`
2. Match filenames against pattern `(.+_src)_(\d{14})\.csv` to extract table name and timestamp
3. Group files by table name
4. Clear the raw folder to prevent duplicate processing
5. Sort by timestamp descending and copy the latest file per table to the raw path
6. Archive all source files into a daily folder for backup and audit
7. Validate successful load using `dbutils.fs.ls(raw_path)`

---

### Bronze Layer — Raw Ingestion

**Purpose:** Store raw data exactly as received from the source, with no transformations.

```sql
USE CATALOG `retail-dwh-project`;
USE SCHEMA bronze;
```

**Tables created:**

| Table | Description |
|---|---|
| `bronze.customers_raw` | Raw customer records |
| `bronze.products_raw` | Raw product records |
| `bronze.stores_raw` | Raw store records |
| `bronze.sales_raw` | Raw sales transaction records |

Files are read directly from S3 using Databricks `read_files()` and loaded into Delta tables without any cleaning or business rules applied.

**Why Bronze?**
- Provides a raw backup for replay and reprocessing
- Supports audit and lineage requirements
- Isolates source data from transformation failures

Row count validation is performed after each load to confirm complete ingestion.

---

### Silver Layer — Data Cleaning

**Purpose:** Clean, standardise, and validate data before warehouse loading.

```sql
CREATE SCHEMA IF NOT EXISTS clean;
```

**Transformations applied:**

| Entity | Transformation |
|---|---|
| Customers | `SELECT DISTINCT` to remove duplicates; `INITCAP(TRIM(CustomerName))` for proper casing; `LOWER(TRIM(Email))` for email standardisation |
| Products | `WHERE UnitPrice > 0` to exclude invalid products |
| Stores | `COALESCE(TRIM(Region), 'Unknown')` to handle missing regions |
| Sales | `TO_DATE(TxnDate, 'dd-MM-yyyy')` to parse and standardise transaction dates |

**Referential integrity:** Sales records are filtered to only include CustomerIDs present in the cleaned customer table.

---

### Gold Layer — Business Warehouse

**Purpose:** Build the analytics-ready Star Schema warehouse with dimension and fact tables.

**Schema type: Star Schema**

One central fact table is connected to four dimension tables. This design avoids over-normalisation (Snowflake schema), resulting in simpler joins and better reporting performance.

**Tables:**

| Table | Type | Description |
|---|---|---|
| `dim_customer` | Dimension | Customer details with SCD Type 2 history |
| `dim_product` | Dimension | Product information with surrogate key |
| `dim_store` | Dimension | Store information with surrogate key |
| `fact_sales` | Fact | Transactional sales joined to all dimensions |

**Surrogate keys** are generated using `ROW_NUMBER()` (e.g., `CustomerSK`, `ProductSK`).

**Fact table amount calculation:**

```sql
Quantity * UnitPrice AS SalesAmount
```

**Duplicate prevention:**

```sql
WHERE TransactionID NOT IN (SELECT TransactionID FROM gold.fact_sales)
```

---

### 00_end — Export Process

**Purpose:** Read final Gold warehouse tables and export them as single CSV files.

**Key steps:**

| Step | Logic | Purpose |
|---|---|---|
| Read Gold tables | `spark.table(full_table)` | Load final Delta warehouse tables |
| Add export timestamp | `withColumn("ExportedAt", current_timestamp())` | Track when each export was generated |
| Single file output | `coalesce(1)` | Write one CSV file instead of multiple Spark partitions |
| File rename | Temporary folder → meaningful filename | Produce human-readable output filenames |
| Cleanup | Delete temporary folders | Keep the output path clean |

Final CSVs are delivered to `s3://retail-etl-dwh-lakehouse/sftp_out/`.

---

## Validation Checks

Validation is applied at both the Silver and Gold layers to catch quality issues early.

### Silver — Customer Validation

| Check | Logic | Purpose |
|---|---|---|
| Multiple city / address per customer | Detect row count > 1 per CustomerID | Identify candidates for SCD Type 2 updates |
| Leading / trailing spaces | `City != TRIM(City)` | Detect formatting issues |
| Uppercase emails | `Email != LOWER(Email)` | Find inconsistent email casing |
| Duplicate customers | `HAVING COUNT(*) > 1` | Identify exact duplicate records |

### Silver — Product Validation

| Check | Purpose |
|---|---|
| Extra spaces in product name | Detects inconsistent formatting |
| `UnitPrice = 0` | Flags invalid business data |

### Silver — Store Validation

| Check | Purpose |
|---|---|
| Missing region | Detects null or blank region values |
| Store name case differences | Detects standardisation issues (e.g., `Store A` vs `STORE A`) |

### Silver — Sales Validation

| Check | Purpose |
|---|---|
| Invalid CustomerID | Sales referencing non-existent customers (referential integrity) |
| Duplicate TransactionID | Duplicate transaction records |
| Quantity = 0 | Invalid sales with no quantity |
| Invalid date format | Improperly formatted transaction dates |

### Gold — Warehouse Validation

| Check | Purpose |
|---|---|
| Null surrogate keys | Confirms all dimension joins resolved correctly |
| Duplicate TransactionID in fact table | Ensures no duplicate sales were loaded |
| One active record per customer | Validates SCD Type 2 — only one `IsActive = 1` per CustomerID |
| Closed SCD records | Confirms historical versions have valid `EndDate` |
| Negative or zero sales amount | Ensures no invalid revenue records |
| Invalid transaction dates | Ensures all fact dates are well-formed |

---

## SCD Type 2 — Historical Tracking

Slowly Changing Dimension Type 2 is implemented on `dim_customer` to preserve the full history of customer address or attribute changes.

**How it works:**

When a customer's record changes (e.g., city or address update):

1. The existing active record is **closed** — `IsActive` is set to `0` and `EndDate` is set to `CURRENT_DATE()`
2. A **new version** is inserted with the updated attributes, a new surrogate key, and `IsActive = 1`

**Key columns:**

| Column | Purpose |
|---|---|
| `CustomerSK` | Surrogate key — unique per version |
| `IsActive` | `1` = current version, `0` = historical version |
| `StartDate` | Date this version became active |
| `EndDate` | Date this version was superseded (null if current) |

**Initial load filter:**

```sql
WHERE CustomerID NOT IN (SELECT CustomerID FROM dim_customer)
```

This ensures only new customers are inserted on first load.

---

## Schema Design

The Gold layer uses a **Star Schema**:

```
                  ┌──────────────┐
                  │  dim_product │
                  └──────┬───────┘
                         │
┌──────────────┐   ┌─────┴──────┐   ┌─────────────┐
│ dim_customer ├───┤ fact_sales ├───┤  dim_store  │
└──────────────┘   └────────────┘   └─────────────┘
```

**Why Star Schema over Snowflake?**

Dimension tables are not further normalised into sub-dimensions. This simplifies join logic and improves query performance for reporting and analytics workloads.

---

## Job Orchestration

A **Databricks Workflow** orchestrates all notebooks in sequence:

```
00_start → Bronze → Silver → Gold → 00_end
```

Each notebook is configured as a dependent task — a downstream notebook only runs if its upstream notebook completes successfully. This ensures pipeline failures are caught early and do not propagate partial data to downstream layers.

---

## Advantages

- **End-to-end automation** — from raw file ingestion through to export, with no manual steps
- **Data quality enforcement** — multi-layer validation catches issues at each stage
- **Historical tracking** — SCD Type 2 preserves the full audit trail of customer changes
- **Scalable cloud architecture** — built on Databricks and AWS S3 for elastic scaling
- **Analytics-ready output** — Star Schema design optimised for BI and reporting tools
- **Replay capability** — Bronze layer retains raw source data for reprocessing
- **Audit support** — daily archive folders and export timestamps provide full lineage

---

## Summary

> This project demonstrates a complete modern data engineering pipeline using Databricks Medallion Architecture. It covers ingestion, cleaning, validation, warehouse modelling, SCD Type 2 handling, and export automation — built entirely on scalable cloud-native technologies.