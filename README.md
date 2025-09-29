# 🗳️ Real-Time Voting Data Engineering Pipeline  

![GitHub repo size](https://img.shields.io/github/repo-size/rakeshgoud12/Real_time_Voting_data_Engineeing_Pipeline?color=blue&style=flat-square)
![GitHub last commit](https://img.shields.io/github/last-commit/rakeshgoud12/Real_time_Voting_data_Engineeing_Pipeline?color=green&style=flat-square)
![GitHub issues](https://img.shields.io/github/issues/rakeshgoud12/Real_time_Voting_data_Engineeing_Pipeline?color=red&style=flat-square)
![GitHub pull requests](https://img.shields.io/github/issues-pr/rakeshgoud12/Real_time_Voting_data_Engineeing_Pipeline?color=orange&style=flat-square)

---

## 🚀 Overview  
This project simulates a **real-time voting system** and showcases how modern data engineering tools can be combined to build a live data pipeline.  

I created this project to **demonstrate end-to-end streaming data processing** — from data generation to real-time analytics and visualization.  

---

## ✨ Features  
- 🔹 **Data Simulation** → Generates fake voter & candidate data  
- 🔹 **Kafka Producer & Consumer** → Streams votes in real-time  
- 🔹 **PostgreSQL** → Stores voters, candidates, and votes  
- 🔹 **PySpark Structured Streaming** → Aggregates votes & turnout in real-time  
- 🔹 **Streamlit Dashboard** → Live visualization of voting results (bar chart, donut chart, stats)  

---

## 🛠️ Project Structure  
.
├── main.py              # Generates candidates & voters, pushes events to Kafka
├── voting.py            # Consumes voter events, records votes, produces vote stream
├── spark-streaming.py   # PySpark job for aggregating votes & turnout
├── streamlit-app.py     # Real-time dashboard with Streamlit
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation

---

## 🧰 Tech Stack  
- **Apache Kafka** → Event streaming backbone  
- **PostgreSQL** → Relational database for persistence  
- **PySpark** → Real-time aggregation with Structured Streaming  
- **Streamlit** → Interactive dashboard for visualization  
- **Python** → Glue for the entire pipeline  

---

## ⚡ How It Works  
1. `main.py` generates candidates & voters, then streams data into Kafka.  
2. `voting.py` consumes voter events → assigns votes → stores in PostgreSQL → produces vote events.  
3. `spark-streaming.py` aggregates votes per candidate & turnout by location.  
4. `streamlit-app.py` displays a **real-time election dashboard** with results.  

---

## 🎥 Demo Preview  

Here’s how the real-time election dashboard looks in action 👇  

![Demo Preview](demo.gif)  

*(You can upload your demo video or screen recording as `demo.gif` or link to a YouTube video)*  

---

## 🎯 Why This Project?  
- To simulate **real-world data pipelines** used in companies like Uber, LinkedIn, and Netflix.  
- To demonstrate **real-time event streaming + analytics** in a clear and visual way.  
- To showcase **end-to-end data engineering skills**: ingestion, storage, processing, and visualization.  

---

👤 **Author:** Rakesh Goud Edigi  
📩 [LinkedIn Profile](https://www.linkedin.com/in/rakeshgoud-edigi)  
