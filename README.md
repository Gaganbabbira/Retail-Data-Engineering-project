## Retail-Data-Engineering-project
Retail Data Engineering Pipeline Using Azure, Databricks And ADLS

## OVERVIEW
this project demonstrates a retail data pipeline built using Azure cloud tools.

## Technologies Used
- Azure Data Lake Storage
- Azure Databricks
- PySpark
- Azure Data Factory
- spark SQL

## Data Pipeline Architecture
Transaction JSON Dataset → Bronze Layer → Silver Layer → Gold Layer

## Layers
1)Bronze Layer
Raw data stored from source JSON dataset.

2)Silver Layer
Cleaned and transformed data.

3)Gold Layer
Final curated dataset used for business metrics and analytics.

## Data Transformation
data transformation was performed using pyspark and spark sql in Azure Databricks.

## Spark SQL was Used For:
-filtering data
-Generating business metrics

## Dataset
The dataset is stored in the data folder in JSON and Parquet formats.
