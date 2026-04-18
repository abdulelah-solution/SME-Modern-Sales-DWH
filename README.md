# 🚀 Upgrading DWH: From Chaos to Clarity (Level 2)
***Transforming Fragmented SME Data into Scalable Business Intelligence***

## 📌 The Problem: The "Data Chaos" in SMEs
Small and Medium Enterprises (SMEs) often struggle with "Data Silos." Sales data lives in a **CRM**, inventory data lives in an **ERP**, and spreadsheets are scattered everywhere.
- **The Result:** Inaccurate profit margins, duplicated customer records, and a "gut-feeling" approach to decision-making instead of a data-driven one.

## 💡 The Solution: Modern Data Stack (ELT)
This project demonstrates a complete migration from a **Monolithic ETL** (Python-heavy) to a **Modern ELT** architecture. By decoupling **Ingestion** from **Transformation**, we create a system that is:
1. **Scalable:** Handles growing data without breaking.
2. **Reliable:** Automated testing ensures the numbers are always right.
3. **Transparent:** Full lineage shows exactly how a raw CSV became a Profit Report.

---

## 🏗️ System Architecture
The project is divided into two major components:
### 1. The Ingestion Engine (Python)
Located in the **`/python`** folder, this layer acts as the **Data Pipeline**. It extracts raw data from CSV files and loads it directly into the **SQL Server (Raw Layer)**.
- **Philosophy:** Keep it simple. Python moves the data; it doesn't touch the logic.

### 2. The Transformation Engine (dbt)
Located in the **`/my_sales_warehouse`** folder, this is where the "magic" happens.
- **Staging:** Cleaning the mess.
- **Intermediate:** Merging CRM & ERP data using **MD5 Hashing** to create unique **Surrogate Keys**.
- **Marts:** Structuring data into a **Star Schema** (Facts & Dimensions) for high-performance analytics.

---

## 📈 Business Value & Benefits
By implementing this architecture, a company gains:
- **Single Version of the Truth:** No more debating which report is correct.
- **Cost Efficiency:** Using **`dbt-core`** keeps infrastructure costs low while maintaining enterprise-grade quality.
- **Fast Insights:** The **Wide Table** (**`fct_sales_wide`**) allows analysts to build PowerBI dashboards in minutes, not days.
- **Future Proofing:** Easily swap a CSV source for a cloud API without rebuilding the entire system.

---

## 🛠️ Tech Stack
- **Language:** Python 3.x (Pandas, SQLAlchemy)
- **Modeling:** dbt-core (Data Build Tool)
- **Database:** Microsoft SQL Server
- **Version Control:** Git

---

## 🚦 How to Initialize the Project
### Phase 1: Data Ingestion
**`python main.py`**

This triggers the Python scripts to load raw data into the staging environment.

### Phase 2: dbt Transformation
**`cd my_sales_warehouse`**

**`dbt deps`**

**`dbt build`**

This command compiles the SQL, materializes the tables, and runs 47 automated data quality tests.

### Project Status: 🔵 Level 2 (Production Ready) | Next Step: Level 3 (Cloud Migration & Orchestration)

---

## 📬 Contact & Collaboration
I am a **Data Analyst / Engineer** passionate about helping SMEs turn their data chaos into profit-driving insights. Let’s connect!
- **LinkedIn:** [**Abdulelah's Profile**](https://www.linkedin.com/in/abdulelah-muhmin-a215a41a1/)
- **GitHub:** [**Abdulelah's Repositories**](https://github.com/abdulelah-solution)