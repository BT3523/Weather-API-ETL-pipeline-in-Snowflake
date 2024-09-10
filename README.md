# Weather-API-ETL-pipeline-in-Snowflake

Weather Data ETL Pipeline
Project Overview
This project is an ETL (Extract, Transform, Load) pipeline designed to integrate weather data from two sources:

Historical Weather Data: Pulled from a CSV file sourced from Kaggle.
Real-Time Weather Data: Pulled from the OpenWeather API.
The pipeline extracts, transforms, and loads this data into a Snowflake cloud data warehouse, combining historical records with real-time weather data for comprehensive analysis.

Features
Data Extraction: Extracts historical weather data from a CSV file and real-time weather data via API requests.
Data Transformation: Standardizes and combines the two datasets into a uniform structure.
Data Loading: The processed data is loaded into a Snowflake table for storage and further analysis.
Multiple Data Sources: Efficiently handles data from both CSV files and live APIs.
Cloud-Based Storage: Utilizes Snowflake, a cloud-based data warehouse, for scalable data management.
Data Sources
Kaggle Historical Weather Data: Kaggle Weather Data (download and save locally as weather_data.csv).
OpenWeather API: Provides real-time weather data. Sign up for an API key from OpenWeather.
