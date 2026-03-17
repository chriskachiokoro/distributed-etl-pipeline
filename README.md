# Distributed Data Processing Pipeline

A cloud-based distributed ETL pipeline built with **Apache Beam** and **Google Cloud Dataflow** to process weekly Reddit CSV data stored in **Google Cloud Storage (GCS)**.

## Project Overview

This project was developed to demonstrate scalable batch data processing in the cloud using GCP services. The pipeline reads raw Reddit data from GCS, validates records, applies business-rule filtering, and routes data into separate output layers for downstream use.

## Features

- Reads weekly CSV data from Google Cloud Storage
- Validates schema and required fields
- Applies transformation and filtering rules
- Separates records into:
  - clean output
  - filtered output
  - quarantine output
  - logs
- Runs on Google Cloud Dataflow using Apache Beam

## Tech Stack

- Python
- Apache Beam
- Google Cloud Dataflow
- Google Cloud Storage

## Repository Structure

```text
distributed-data-processing-pipeline/
│
├── pipeline.py
├── requirements.txt
├── README.md
│
├── report/
│   └── cookoro-finalproject.pdf
│   └── cookoro-finalproject.docx
├── raw/
│   └── week1.csv
│   └── week2.csv
│   └── week3.csv
│   └── week4.csv
│   └── week5.csv
│
└── screenshots/
    ├── dataflow-job-graph.png
    ├── gcs-buckets.png
    ├── output-files.png
    └── pipeline-results.png
