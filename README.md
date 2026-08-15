                                                 Project Overview

The suite operates as a digital "sentinel," continuously monitoring the heartbeat of the Nepalese economy. It eliminates manual data collection by using automated scrapers and orchestrators to maintain a fresh data lake for financial analysis.

                                      Core Components:

NEPSE Sentinel: Tracks daily stock transactions, floor-sheet data, and corporate actions.

Macro-Economic Intelligence: Monitors inflation, remittance, and liquidity ratios directly from NRB's official reports.

                                         Tech Stack & Architecture

This project follows the Medallion Architecture (Bronze → Silver → Gold) to ensure data quality and reliability.

Extraction: Custom Python scrapers built with BeautifulSoup and Selenium.

Orchestration: Apache Airflow managing daily DAGs to ensure reliable ETL execution.

Data Processing: Pandas and NumPy for cleaning and feature engineering.

Machine Learning: Scikit-learn for predictive modeling (e.g., Sales or Price prediction).

Deployment: Streamlit and Gradio for interactive web-based dashboards.

                                                           Key Features

Automated ETL Pipelines: Fully automated workflows that handle data ingestion with zero manual intervention.

Financial Sentiment Analysis: Utilizing market trends to gauge investor sentiment.

Macro-Economic Dashboards: Real-time visualization of remittance and inflation trends in Nepal.

Production-Ready Models: Integrated ML models (like the Big Mart Sales Predictor) deployed via cloud-native tools.

                                        Repository Structure

nepal-economic-sentinel/
├── airflow/             # DAGs and task scheduling
├── dbt/                 # SQL transformations and models
├── ingestion/           # Python scraping & streaming scripts
├── spark/               # PySpark batch jobs
├── docker-compose.yml   # Infrastructure orchestration
└── README.md
