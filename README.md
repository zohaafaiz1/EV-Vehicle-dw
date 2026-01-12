# EV Vehicle Data Warehouse & Analytics

End-to-end data analytics project implementing ETL/ELT pipelines, dimensional data modeling, indexing optimization, and analytical reporting.

---

## Project Overview
This project demonstrates how raw EV-related operational data is transformed into a structured data warehouse and analyzed for decision support.  
The system is optimized for DSS workloads involving aggregations, joins, and multi-dimensional queries.

---

## Architecture
- Source relational data
- ETL / ELT processing
- Star schema data warehouse (PostgreSQL)
- Indexing for performance optimization
- Analytical queries and reporting (Power BI)

---

## ETL / ELT
- Data extraction from source tables
- Cleaning and transformation of raw data
- Key generation and normalization
- Loading into fact and dimension tables
- Transformations applied using SQL (ELT where applicable)

---

## Data Warehouse Design
- Database: PostgreSQL
- Schema: Star Schema
- Fact table for EV usage / transactions
- Dimension tables for users, vehicles, and attributes

---

## Indexing & Performance
- B-Tree indexes for selective and range queries  
  - ~65–70% runtime improvement
- Bitmap indexes for multi-condition filtering  
  - ~60% performance improvement
- Index scans consistently outperformed sequential scans

---

## Workload Considerations
- DSS: Aggregations, joins → B-Tree + Bitmap indexes
- OLTP: Single-row operations → B-Tree only

---

## Reporting
- Analytical queries executed on warehouse tables
- Insights presented via reports and dashboards
- Interactive filtering and trend analysis

---

## Tech Stack
- PostgreSQL
- SQL
- Power BI

---

## Repository Structure
