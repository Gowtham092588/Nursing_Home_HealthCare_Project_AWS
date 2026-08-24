# Welcome to Nursing Home HealthCare Analytics Project!

## 📖 Overview

 This project implements a production-ready data pipeline for analyzing Healthcare Metrics Project. It ingests, transforms, aggregates, and visualizes nursing home provider, staffing, vaccination datasets using Python, S3, Amazon Glue, Athena, StreamLit following medallion architecture and SCD Type 2 dimension using Delta Lake .

## 🎯 Business Objective 

* centralized dashboard that helps healthcare organizations understand staffing, provider capacity, vaccination coverage across facilities and states.
* identify trends, compare performance, and support better workforce planning and operational decision-making.

## 🚀 Project Highlights

* AWS Glue Based Ingestion and Transformation.
* Medallion architecture (Bronze → Silver → Gold).
* Decoupled AWS Glue job architecture across Bronze, Silver, and Gold layers.
* SCD Type 2 Dimension.
* Data quality validation.
* Delta Lake optimized storage.
* AWS Glue Workflow for Orchestration and Scheduling.
* Amazon Athena for querying curated datasets.
* Interactive streamlit dashboard.

## 🏛️ Architecture
This project implements medellion architecture with delta lake storage:

<img width="975" height="316" alt="image" src="https://github.com/user-attachments/assets/28579d53-15e6-4323-9958-4324f7a74c0b" />



