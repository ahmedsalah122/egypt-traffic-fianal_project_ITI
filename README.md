# TrafficPulse Egypt 🚦

TrafficPulse Egypt is a real-time traffic and incidents monitoring platform designed to provide live visibility into road congestion and incidents across Cairo.

## 📌 Problem Statement

Traffic incidents and congestion occur continuously across Cairo's road network. Decision-makers often rely on incomplete or outdated information, making incident response and traffic management difficult.

## 💡 Solution

TrafficPulse Egypt collects live traffic and incident data, processes it using a streaming data pipeline, and delivers actionable insights through interactive dashboards.

---

## 🛠 Tech Stack

- Python
- Apache Kafka (KRaft Mode)
- Spark Structured Streaming
- AWS S3
- PostgreSQL + PostGIS
- Grafana
- Docker

---

## Architecture

```text
   TomTom API 
        ↓
Python Producer
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Bronze → Silver → Gold
        ↓
PostgreSQL + PostGIS
        ↓
Grafana Dashboard
```

---

## Features

- Real-Time Traffic Monitoring
- Incident Detection
- Sliding Window Aggregations
- Spatial Analytics with PostGIS
- Interactive Dashboards
- Fault-Tolerant Streaming Pipeline

---

## Project Structure

```text
TrafficPulse-Egypt/
│
├── architecture/
├── docs/
├── grafana/
├── postgres/
├── producer/
├── schemas/
├── spark/
├── README.md
├── docker-compose.yml
└── Dockerfile
```

---

## Team

- Ahmed Salah
- Mohamed Heikal
- Omar Ayyad
- Mostafa Azab
