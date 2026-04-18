# 📂 My Sales Warehouse (dbt Project)
Welcome to the core transformation engine of the **Upgrading DWH** project. This directory contains the **dbt (Data Build Tool)** implementation, representing a shift from traditional ETL to a modern **ELT** architecture.

---

## 🚀 The Evolution: From Level 1 to Level 2
This project marks my transition from manual scripting to professional data modeling.

| Feature           | 🔴 Level 1: Classic ETL (Previous)          | 🔵 Level 2: Modern ELT (Current)
| ----------------- | -------------------------------------------- | -------------------------------------------------
| **Tools**         | Python + SQL Server                          | Python + dbt-core + SQL Server
| **Approach**      | Python did everything (Extract, Clean, Load) | Python handles EL, dbt handles T (Transformation)
| **Maintenance**   | Hard; Monolithic and complex code            | Easy; Modular, clean SQL files
| **Quality**       | Manual/exhausting testing                    | Automated **`dbt test`** (Unique, Not Null, etc.)
| **Documentation** | Manual or non-existent                       | Auto-generated via **`dbt docs`**

---

## 🏗️ Data Architecture & Modeling
I implemented a **Medallion-style Architecture** using dbt layers to ensure data integrity and scalability:
1. **🥉 Staging Layer (`models/staging/`)**
   - **Purpose:** Clean, cast data types, and rename columns from raw sources (**`CRM`** & **`ERP`**).
   - **Source Alignment:** Organized by source system to maintain clear lineage.

2. **🥈 Intermediate Layer (`models/intermediate/`)**
   - **Purpose:** The "Engine Room." This is where complex joins and business logic happen.
   - **Mastering Identity:** Implemented **Surrogate Keys (MD5 Hashing)** via **`dbt_utils`** to create unique identifiers across disparate systems.

3. **🥇 Marts Layer (`models/marts/`)**
   - **Purpose:** The Gold standard. Data is structured into a **Star Schema** for end-user consumption.
   - **Output:** **`dim_customers`**, **`dim_products`**, and **`fct_sales`**.
   - **Bonus:** Created a **`fct_sales_wide`** (Denormalized Table) to provide a "ready-to-use" view for PowerBI analysts.

---

## 🛠️ Key Technical Features
- **Modular SQL:** Instead of 1,000 lines of Python, logic is broken into small, testable dbt models.
- **Data Quality Assurance:** Every model is protected by **`dbt tests`**. We ensure referential integrity (Relationships) and uniqueness before the data ever reaches a dashboard.
- **Documentation as Code:** All column descriptions and business logic are embedded in **`.yml`** files, accessible via the **`dbt docs`** UI.

---

## 💻 How to Run (Development Phase)
Currently, the workflow is executed manually to ensure full control during the development lifecycle:
1. **Ingest:** Run the Python script in the root directory to load CSVs into the Raw layer.
2. **Install Dependencies:**
**`dbt deps`**
3. **Build & Test:**
**`dbt build`**
(This command runs **`dbt run`** and **`dbt test`** sequentially to ensure only high-quality data is materialized).
4. **View Documentation:**
**`dbt docs generate && dbt docs serve`**
#### Status: 🔵 Level 2 Completed Successfully. Ready for Analytical Insights.

---

## 📬 Connect with Me
If you have any questions about this project or want to discuss Data Engineering, feel free to reach out:
- **LinkedIn:** [**Abdulelah's Profile**](https://www.linkedin.com/in/abdulelah-muhmin-a215a41a1/)
- **GitHub:** [**Abdulelah's Repositories**](https://github.com/abdulelah-solution)