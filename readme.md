# 🚀 How to Deploy Scalable GenAI Apps on Databricks using FastAPI

![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
![Status](https://img.shields.io/badge/status-in%20progress-yellow)
![Platform](https://img.shields.io/badge/platform-Databricks-orange)

## Overview

This repository demonstrates building and deploying a scalable and secure GenAI-powered ChatBot using FastAPI on Databricks Apps, powered by OpenAI models and Databricks Genie Conversational APIs.

---

## Features

- Ingest CMS provider dataset
- Create Genie Spaces for semantic querying
- Build a search + chat experience using FastAPI
- Deploy and monitor as a scalable Databricks App

---

## Dataset

**CMS DAC_NationalDownloadableFile.csv** includes:

- Facility names and locations
- Provider types, specialties, and services
- Ownership details
- Certification dates

Dataset Reference:[CMS Provider Dataset](https://data.cms.gov/provider-data/dataset/mj5m-pzi6)

---

## Why Databricks?

| Feature           | Purpose                                        |
|-------------------|------------------------------------------------|
| Delta Lake        | Scalable ingestion of large healthcare datasets |
| Genie Spaces      | Conversational agent with semantic querying     |
| Unity Catalog     | Secure governance and data access               |
| Databricks Apps   | Instant scalable API deployment                 |
| MLflow + DBSQL    | Monitoring and observability                    |

---

## Quick Start

1. Clone this repository to your workspace.
2. Update the `.env` file with your Databricks workspace URL and personal access token.
3. Review all files: environment config, app logic, helper functions, table extraction utility, and HTML templates.
4. Install dependencies listed in `requirements.txt`.
5. Deploy on Databricks Apps:
   - Go to Apps → New → Custom → Name your app (e.g., fastapi-genie)
   - Upload all files and deploy
6. Test the endpoint by accessing the app UI and asking questions.

---

## File Structure

- `.env` – Environment variables
- `app.yaml` – App deployment configuration
- `requirements.txt` – Python dependencies
- `app.py` – Main FastAPI application
- `helper.py` – Functions to call Genie endpoints
- `table_extraction.py` – Extracts table names from queries
- `templates/index.html` – Frontend UI

---

## Deployment Steps

1. Create a Custom Application in Databricks Apps.
2. Upload all code files.
3. Click Deploy and monitor deployment status.
4. Test the application by opening its endpoint and validating responses from Genie Spaces.

---

## Conclusion

Using Genie Spaces, CMS datasets, FastAPI, and Databricks Apps, you can deploy scalable GenAI applications for semantic querying of nationwide healthcare provider data without managing any infrastructure.

---

## Authors & Reviewers

- **Author:** Nitin Aggarwal

---

## References

- [Databricks Apps](https://www.databricks.com/product/databricks-apps)
- [Genie Spaces](https://www.databricks.com/product/business-intelligence/ai-bi-genie)
- [CMS Provider Dataset](https://data.cms.gov/provider-data/dataset/mj5m-pzi6)
- [FastAPI Docs](https://fastapi.tiangolo.com/)

---

## License

This project is licensed under the Apache 2.0 License.

---

Built with ❤️ by Nitin Aggarwal
