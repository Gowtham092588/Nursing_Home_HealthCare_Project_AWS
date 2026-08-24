# Welcome to Nursing Home HealthCare Analytics Project!

## 📖 Overview

 This project implements a production-ready data pipeline for analyzing Healthcare Metrics Project. It ingests, transforms, aggregates, and visualizes nursing home provider, staffing, vaccination datasets using Python, S3, Amazon Glue, Athena, StreamLit following medallion architecture and SCD Type 2 dimension using Delta Lake .

## 🎯 Business Objective 

* centralized dashboard that helps healthcare organizations understand staffing, provider capacity, vaccination coverage across facilities and states.
* identify trends, compare performance, and support better workforce planning and operational decision-making.

## 🚀 Project Highlights

☑️ AWS Glue Based Ingestion and Transformation.
☑️ Medallion architecture (Bronze → Silver → Gold).
☑️ Decoupled AWS Glue job architecture across Bronze, Silver, and Gold layers.
☑️ SCD Type 2 Dimension.
☑️ Data quality validation.
☑️ Delta Lake optimized storage.
☑️ AWS Glue Workflow for Orchestration and Scheduling.
☑️ Amazon Athena for querying curated datasets.
☑️ Interactive streamlit dashboard.

## 🏛️ Architecture
This project implements medellion architecture with delta lake storage:

<img width="975" height="316" alt="image" src="https://github.com/user-attachments/assets/28579d53-15e6-4323-9958-4324f7a74c0b" />

## 🏗️ Bronze → Silver → Gold Architecture

### 🟫 Bronze Layer – Raw Data Ingestion
- Ingests healthcare source datasets from Amazon S3 into Delta Lake tables
- Preserves source data with minimal transformation for traceability
- Maintains independent Bronze tables for Provider, Staffing, State Average, and Vaccination datasets
- Provides the raw data foundation for downstream Silver processing

### ⬜ Silver Layer – Cleansed & Curated Data
- Cleans and standardizes data types, nulls, whitespace, text formatting, and column names
- Applies reusable data quality validations, duplicate handling, and quarantine of invalid records
- Implements SCD Type 2 for Provider data to preserve historical attribute changes
- Produces trusted, independently processed Silver datasets for downstream analytics

### 🟨 Gold Layer – Business & Analytics Data
- Builds business-ready metrics from validated and curated Silver datasets
- Produces staffing, bed utilization, care-hours, nurse-to-patient, state healthcare, and vaccination metrics
- Organizes transformations into independent Gold domains for decoupled processing
- Publishes analytics-ready Delta tables for Amazon Athena and the Streamlit dashboard

### 🛠️ Technologies Used

- **AWS Glue & PySpark**
- **Amazon S3 & Delta Lake**
- **AWS Glue Workflows, Crawlers & Data Catalog**
- **Amazon Athena, Streamlit** 
- **Python, Pandas, Git & GitHub** 



