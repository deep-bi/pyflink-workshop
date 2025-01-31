# Apache Flink Workshop for Python Developers

This repository contains a step-by-step introduction to Apache Flink, concepts, and patterns.
You are free to follow the instructions and code along with the presenter or just listen and learn.

If you want to follow then you need to have the following on your machine:

* Docker Compose - for running Flink and Kafka
* Python 3.8 - 3.11 development environment (tested using Python 3.10)
* [Optional] Java 11 - for running Flink locally

# Chapters

### Workshop 1

* [Chapter 1 - Introduction to Flink Stream Processing](chapter-1/readme.md)
* [Chapter 2 - Window Functions](chapter-2/readme.md)
* [Chapter 3 - Stream Joins](chapter-3/readme.md)
* [Chapter 4 - Connectors](chapter-4/readme.md)
* [Chapter 5 - Real-Time Data Deduplication](chapter-5/readme.md)

### Workshop 2

* Chapter 6 - Real-Time Anomaly Detection with Control Streams
* Chapter 7 - Complex Metric Monitoring and Alerting Using Custom Window Functions
* Chapter 8 - Using AI Models for On-the-Fly Inference

# Setup

#### Python environment with dependencies

You can use a virtual environment for the Python dependencies. For example:

```
python3 -m venv .venv
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Java 11

There are multiple ways to install JDKs but the most convenient and easiest is [SDKMAN](https://sdkman.io/).
You can use it to install specific JDK and update all paths and ENV variables associated with Java.

To install SDKMAN run: `curl -s "https://get.sdkman.io" | bash`. Then type: `sdk list java` to
see what JDKs are available. E.g. to install `11.0.26-tem` just run:
`sdk install java 11.0.26-tem`.

## Starting Flink and Kafka clusters

To start Flink and Kafka clusters use Docker Compose:

```bash
docker compose up
```

Flink is available at <http://localhost:8081/>, while Kafka UI at <http://localhost:8080/>.