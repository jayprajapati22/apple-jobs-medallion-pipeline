# Apple Jobs Medallion Pipeline


This project demonstrates a full Data Engineering pipeline using the Medallion Architecture (Bronze, Silver, Gold) with PySpark. It uses job listing data from Apple Inc. to build an analytical-ready dataset.

Architecture Used
Raw CSV (apple_jobs.csv)
        │
        ▼
🟫 Bronze Layer – Raw Ingested Data + Metadata
        │
        ▼
⬜ Silver Layer – Cleaned & Transformed Data
        │
        ▼
🥇 Gold Layer – Aggregated Data for Analytics

🧪 Technologies Used
PySpark (ETL & DataFrames)
Databricks / Spark Notebooks
Parquet Format
Medallion Architecture

📝 Steps Performed
1. 📥 EDA (Exploratory Data Analysis)
Schema & null value analysis
Count of unique roles, locations, teams
Most common countries & job types
Top cities by job openings
WordCloud of job titles

2. 🟫 Bronze Layer
Read raw CSV with inferred schema
Add metadata: ingestion_timestamp, source_file
Rename columns to snake_case
Store as Parquet

3. ⬜ Silver Layer
Drop rows with nulls in critical columns
Standardize text (lowercase, trim)
Extract city and country from location
Add job_type column (Intern, Manager, Full-Time)
Save cleaned data as Parquet

4. 🥇 Gold Layer (Coming Soon)
Group by country, team, and job_type
Generate job counts, top hiring cities, etc.

📈 Example Use Cases
Where are the most Apple jobs posted globally?
What teams are hiring the most?
Intern vs Full-Time vs Manager role analysis
Country-wise hiring breakdown

✅ Future Improvements
Add Gold Layer aggregations & dashboards
Add Delta Lake format for versioning
Integrate with Power BI / Tableau
Automate pipeline with Airflow or Databricks workflows

👨‍💻 Author
Jay Prajapati
DWT Engineer | Infosys | Pune
