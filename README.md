# NYC Yellow Taxi Trips Analysis

This project analyzes NYC Yellow Taxi trips for the year 2022 using **PySpark**. It processes raw data, cleans it, and derives meaningful insights into trip patterns,
passenger behavior, and trip attributes. The project is designed to demonstrate the end-to-end data pipeline, from data ingestion to insightful analytics.

## Table of Contents

1. [Overview](#overview)
2. [Data Sources](#data-sources)
3. [Project Workflow](#project-workflow)
4. [Features and Insights](#features-and-insights)
5. [Setup](#setup)
6. [Usage](#usage)
7. [Results](#results)

---

## Overview

The project utilizes PySpark to process and analyze NYC Yellow Taxi trip data. The workflow involves:
- Downloading raw trip data and zone lookup information.
- Moving the data to HDFS for distributed processing.
- Cleaning and transforming the data to prepare for analysis.
- Analyzing trip patterns to uncover insights like:
  - Monthly and weekly trip distributions.
  - Preferred payment methods.
  - Popular pickup zones.

---

## Data Sources

1. **NYC Yellow Taxi Trip Data**: Monthly parquet files for 2022, sourced from [GitHub](https://github.com/KareeemBeltagy/NYC_TCL_Taxi).
2. **Taxi Zone Lookup**: A CSV file mapping location IDs to zones and boroughs.

---

## Project Workflow

1. **Data Download**: 
   - Parquet files and zone lookup CSV are downloaded from a GitHub repository.

2. **Data Transfer**:
   - Files are moved from the local system to HDFS for distributed storage.

3. **Spark Session Creation**:
   - A Spark session is initialized for data processing.

4. **Data Processing**:
   - Read raw data into Spark DataFrames.
   - Clean the data by handling null values and filtering outliers.
   - Add derived columns like `duration`, `day_name`, and `trip_distance` (in kilometers).

5. **Data Partitioning and Storage**:
   - Save the cleaned data to HDFS, partitioned by `year` and `month` for efficient querying.

6. **Analysis**:
   - Monthly and weekly trip distributions.
   - Trip distribution by boroughs and zones.
   - Insights on payment types and passenger counts.
   - Categorization of trips by distance.

---

## Features and Insights

### Key Functionalities:
- **Data Cleaning**:
  - Handle missing values in columns like `passenger_count` and `total_amount`.
  - Exclude trips with unreasonable distances or amounts.
  - Ensure all numeric columns have non-negative values.

- **Data Enrichment**:
  - Compute trip durations.
  - Partition data by `year` and `month`.

- **Exploratory Analysis**:
  - Distribution of trips by month and day of the week.
  - Top pickup zones in Manhattan.
  - Preferred payment methods.
  - Trip categorization based on distance (short, medium, long).

### Insights:
- Most trips occur in **Manhattan** (89% of total trips).
- **Saturday** has the highest number of trips.
- **Credit cards** are the most popular payment method.
- The majority of trips are **short (under 2 miles)**.

---

## Setup

### Prerequisites
- **PySpark**: Ensure Spark is installed and accessible.
- **HDFS**: HDFS setup is required for data storage.
- **Python Libraries**: `requests`, `os`, and `subprocess`.

### Installation
Clone the repository:
```bash
git clone [https://github.com/<your-repo>/NYC_Yellow_Taxi_Analysis.git](https://github.com/KareeemBeltagy/NYC_TCL_Taxi.git)
cd NYC_Yellow_Taxi_Analysis

### Usage
run yellow_trips script using spark submit:
```bash
spark-submit --master yarn yellow_trips.py
