# 🌤️ Beverly Hills Weather ETL Pipeline

This project implements an ETL pipeline that fetches real-time weather data for Beverly Hills, CA, from the OpenWeatherMap API, transforms relevant metrics, and loads the data into a PostgreSQL database for archival and analytics.

## 🌍 Purpose

- Monitor localized climate conditions for Beverly Hills
- Support visualization and trend analysis on temperature, humidity, and weather patterns
- Practice modular ETL design and Airflow orchestration for API-driven data workflows

## 🧰 Tech Stack

| Component        | Purpose                        |
|------------------|--------------------------------|
| Python           | Core ETL logic and scripting               |
| Requests         | HTTP client for API interaction                |
| Pandas           | Data wrangling, transformation, and cleaning   |
| SQLAlchemy       | Database ORM and connectivity abstraction           |
| PostgreSQL       | Data warehouse / structured data storage               |
| Apache Airflow   |Workflow orchestration and DAG scheduling |
| Metabase       	 | Business Intelligence (BI) & data visualization |
| Linus	Server     | Hosting / runtime environment|
| Github	         |Version control and CI/CD pipelines                              |
| OpenWeatherMap API | External weather data source                    |
