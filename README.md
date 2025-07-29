# 🌤️ Beverly Hills Weather ETL Pipeline

This project implements an ETL pipeline that fetches real-time weather data for Beverly Hills, CA, from the OpenWeatherMap API, transforms relevant metrics, and loads the data into a PostgreSQL database for archival and analytics.

## 🌍 Purpose

- Monitor localized climate conditions for Beverly Hills
- Support visualization and trend analysis on temperature, humidity, and weather patterns
- Practice modular ETL design and Airflow orchestration for API-driven data workflows

## 🧰 Tech Stack

| Component        | Purpose                        |
|------------------|--------------------------------|
| Python           | Core ETL logic                 |
| Requests         | API interaction                |
| Pandas           | Data wrangling & cleaning      |
| SQLAlchemy       | DB connectivity                |
| PostgreSQL       | Data warehouse                 |
| Apache Airflow   | DAG orchestration & scheduling |
| Metabase       	 | Business Intelligence / Visualization |
| Linus	Server     | Server                                |
| Github	         |CICD                                   |
| OpenWeatherMap API | Data source                         |
