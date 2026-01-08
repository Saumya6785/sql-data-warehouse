# 🏢 Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project demonstrates a **comprehensive data warehousing and analytics solution**, from building a modern data warehouse to generating actionable business insights.

Designed as a **portfolio project**, it showcases **industry best practices** in data engineering, ETL pipelines, and analytics.

---

## 🏗️ Data Architecture

The project follows the **Medallion Architecture** approach with **Bronze, Silver, and Gold layers**.

### 🔹 Bronze Layer
- Stores **raw data as-is** from source systems  
- Data ingested from **CSV files** into **SQL Server**

### 🔹 Silver Layer
- Performs **data cleansing, standardization, and normalization**
- Prepares data for analytical processing

### 🔹 Gold Layer
- Contains **business-ready data**
- Modeled using a **star schema** for reporting and analytics

---

## 📖 Project Overview

This project includes:

- **Data Architecture**  
  Designing a modern data warehouse using the Medallion Architecture

- **ETL Pipelines**  
  Extracting, transforming, and loading data from source systems

- **Data Modeling**  
  Developing **fact and dimension tables** optimized for analytics

- **Analytics & Reporting**  
  Creating SQL-based reports and queries for actionable insights

---

## 🎯 Who Is This Project For?

This repository is ideal for professionals and students aiming to showcase skills in:

- SQL Development  
- Data Architecture  
- Data Engineering  
- ETL Pipeline Development  
- Data Modeling  
- Data Analytics  

---

## 🛠️ Tools & Resources

Everything used in this project is **free** ✅

- **Datasets**: CSV files for ERP and CRM systems  
- **SQL Server Express**: Lightweight SQL Server for data warehousing  
- **SQL Server Management Studio (SSMS)**: Database management and querying  
- **GitHub**: Version control and project collaboration  
- **Draw.io**: Data architecture, models, and ETL flow diagrams  
- **Notion**: Project templates and structured task tracking  

---

## 🚀 Project Requirements

### 🏗️ Building the Data Warehouse (Data Engineering)

#### 🎯 Objective
Develop a **modern data warehouse** using SQL Server to consolidate sales data and support analytical reporting.

#### 📌 Specifications
- **Data Sources**:  
  Two source systems (ERP and CRM) provided as CSV files
- **Data Quality**:  
  Clean and resolve data quality issues before analysis
- **Integration**:  
  Merge both sources into a **single analytical data model**
- **Scope**:  
  Focus on the **latest data only** (no historization required)
- **Documentation**:  
  Provide clear documentation for business and analytics users

---

### 📊 BI: Analytics & Reporting (Data Analysis)

#### 🎯 Objective
Develop SQL-based analytics to deliver insights into:

- Customer Behavior  
- Product Performance  
- Sales Trends  

These insights support **data-driven decision-making** for stakeholders.

---

## 📂 Repository Structure
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project


