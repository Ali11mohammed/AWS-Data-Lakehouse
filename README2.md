
# STEDI Human Balance Analytics - Data Lake Project

This project is part of a data engineering pipeline that ingests and processes sensor and customer data using AWS Glue, Spark, and Amazon S3. The primary goal is to build a set of trusted and curated datasets for machine learning purposes, while ensuring user privacy and consent.

---

## 📊 Project Objective

To process raw data from different sources (Landing Zone) and build trusted and curated tables to be used for advanced analytics or machine learning.

---

## 📁 Datasets

1. **customer_landing**: Raw customer data from the website.
2. **accelerometer_landing**: Raw accelerometer sensor readings.
3. **step_trainer_landing**: IoT sensor data from fitness step trainers.

---

## 🔄 Project Steps

### Step 1: Create Trusted Zones

#### ✅ `customer_trusted`
- **Goal**: Include only customers who agreed to share data (`sharewithresearchasofdate` is not null).
- **Data Source**: `customer_landing`
- **Storage**: S3 → Glue Table: `customer_trusted`

#### ✅ `accelerometer_trusted`
- **Goal**: Include only accelerometer records for customers who agreed to share data.
- **Data Source**: `accelerometer_landing` + filter users from `customer_trusted`
- **Storage**: S3 → Glue Table: `accelerometer_trusted`

#### ✅ `step_trainer_trusted`
- **Goal**: Include only step trainer records from customers who have accelerometer data and agreed to share.
- **Data Source**: `step_trainer_landing` + filter using `customer_trusted`
- **Storage**: S3 → Glue Table: `step_trainer_trusted`

---

### Step 2: Create Curated Zones

#### ✅ `customers_curated`
- **Goal**: Keep only customers that have accelerometer data and consented.
- **Join**: `customer_trusted` with `accelerometer_trusted` on `email = user`
- **Storage**: S3 → Glue Table: `customers_curated`

#### ✅ `machine_learning_curated`
- **Goal**: Join:
  - `accelerometer_trusted` and `step_trainer_trusted` on timestamp
  - Retain only records of customers from `customers_curated`
- **Storage**: S3 → Glue Table: `machine_learning_curated`

---

## 🧪 Row Count Validation 

| Table                   | Count |
|------------------------|----------------|
| customer_landing       | 956            |
| accelerometer_landing  | 81273          |
| step_trainer_landing   | 28680          |
| customer_trusted       | 482            |
| accelerometer_trusted  | 40981          |
| step_trainer_trusted   | 14460          |
| customers_curated      | 482            |
| machine_learning_curated | 43681        |

---

## 🛠️ Technologies Used

- AWS Glue (ETL)
- AWS S3 (Storage)
- AWS Athena (SQL Queries)
- AWS Glue Data Catalog
- Apache Spark (via Glue jobs)
