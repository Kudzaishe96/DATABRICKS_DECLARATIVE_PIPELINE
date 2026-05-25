# Databricks_Data_warehouse
Platform Databricks and using meta data

## ETL project using Data Bricks

## Table Of Contents

- [ Project Overview ](#Project-Overview)
- [ Data Source ](#Data-Source)
- [ Tools ](#Tools)
- [ Data Extraction (Bronze Layer) ](#Data-Extraction-(Bronze-Layer))
- [ Data Cleaning (Silver Layer) ](#Data-Cleaning-(Silver-Layer))
- [ Gold Layer](#Gold-Layer)


### Project Overview

This project seek to show the extraction of raw data into meaningfull sql views for data analytics 


### Data Source
EMR and CMR Data csv files uploaded into databricks wokrkspace

### Tools
- Databricks
- Databricks Catalog
- SQL
- Python


## Medallion Architecture

### Load Data(Bronze Layer)
1. Load csv files into Databricks Catalog as source 
2. Create a ctrl.metadata.pipeline table to act as reference or external tables and define the relative tables for bronze layer
3. Create the 1st pipeline job.
4. Create the bronze layer script that loads the bronze layer.
5. Run your first data pipeline to materialise tables.


### Data Cleaning (Silver Layer)
1. Create a ctrl.metadata.silver table to act as reference or external tables and define the relative tables for the silver layer.
2. Run the pipeline
   
### Gold Layer
1. Create a ctrl.metadata.silver table to act as reference or external tables and define the relative tables for the gold layer.
2. Using the Kimball Model , combine tables from the silver to create dim_customers,dim_products and gold_facts while following the business rules and naming conventions
   ### *Gold Layer Schema*
   <img width="781" height="558" alt="CRM   ERP Schema drawio" src="https://github.com/user-attachments/assets/8f9f516f-35cd-4184-886d-34c58acd296f" />

