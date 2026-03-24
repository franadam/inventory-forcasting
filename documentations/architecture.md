# Architecture

- **PostgreSQL** : OLTP source and data warehouse
- **Containerization** (Docker): Build Docker containers that include Python, Airflow, and necessary libraries. Ensure that the environment is reproducible and consistent across development and production.
- **Apache Airflow**: Manages end-to-end data ingestion pipelines, coordinating tasks such as data download, transformation, and loading into the data warehouse.
- **PySpark** : data transformations 
- **Power BI** : consommation
