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
- **Notion**: Project templates and structured task tracking  (https://www.notion.so/Data-Warehouse-Project-2d8326e98b52801abb5bf91f72d03883) for this project

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
├── datasets/ # Raw datasets used for the project (ERP and CRM data)
│
├── docs/ # Project documentation and architecture details
│ ├── etl.drawio # ETL techniques and methods
│ ├── data_architecture.drawio # Overall data architecture
│ ├── data_catalog.md # Dataset catalog with field descriptions
│ ├── data_flow.drawio # Data flow diagram
│ ├── data_models.drawio # Star schema data models
│ ├── naming-conventions.md # Naming standards
│
├── scripts/ # SQL scripts for ETL and transformations
│ ├── bronze/ # Raw data ingestion scripts
│ ├── silver/ # Data cleaning and transformation scripts
│ ├── gold/ # Analytical and reporting models
│
├── tests/ # Data validation and quality checks
│
├── README.md # Project overview and usage instructions
├── LICENSE # License information
├── .gitignore # Git ignored files and folders
└── requirements.txt # Project dependencies and requirements



## 🙏 Acknowledgements & Credits

- Data architecture and ETL design concepts were inspired by **Datawithbaraa**.
- Draw.io diagrams were adapted and customized from reference visuals created by **him as well**.
- This project is an **original implementation**, with modifications, restructuring, and enhancements made for learning and portfolio demonstration purposes.

