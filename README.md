
# 🎬 End-to-End Data Engineering Project: Movie Data Pipeline

This data engineering project simulates a real-world batch processing data pipeline using **Apache Airflow**, **Apache Beam**, **PySpark**, **MySQL**, and **Power BI**. The pipeline collects daily movie data from the **TMDB** and **IMDb** APIs, processes it through a Dockerized ETL workflow, stores cleaned data in **MySQL**, and visualizes insights using **Power BI**.

---

## 📚 Technologies Used

| Component         | Tool/Service              |
|------------------|---------------------------|
| Workflow Engine  | Apache Airflow (Docker)   |
| Batch & Streaming| Apache Beam (DirectRunner)|
| Data Processing  | PySpark                   |
| Data Storage     | MySQL (Dockerized)        |
| Dashboarding     | Power BI                  |
| Containerization | Docker, Docker Compose    |
| Scheduler        | Airflow DAGs              |

---

## 🔄 Pipeline Flow

![image](https://github.com/user-attachments/assets/99ef3637-775f-4ff7-bad1-500a080d93ca)


---

## 🔧 Features Implemented

### 🔹 1. **Data Ingestion**
- Fetches data daily from:
  - **TMDB Trending Movies API**
  - **IMDb Most Popular Movies API** (via RapidAPI)
- Handles pagination, headers, and rate limiting.

### 🔹 2. **Cleaning & Normalization (Apache Beam)**
- **TMDB**: Cleaned fields such as `title`, `release_date`, `popularity`, and `genre_ids`.
- **IMDb**: Parsed fields including `vote_count`, `metascore`, `rating`, and `duration`.
- Unified schema across both sources using common fields like `source`, `movie_id`, `title`, `genres`, `vote_count`, `rating`, etc.

### 🔹 3. **Merge and Normalization**
- Merged the normalized data into a single CSV file with a unified schema.
- Applied a **normalization technique** to compute a `normalized_score` for each movie.
- Normalized scores were calculated separately for TMDB and IMDb before merging, and then recalculated after merging to ensure consistency.
- Details of the normalization methodology are documented in `normalization.txt`.
- If we have less than 10 records in total then we will not trigger the downstream task insted an email will be sent using the `Email operator`.
  *_please note that the email operator is currently a dummy operator. I have removed this operator here due to security reasons._

### 🔹 4. **Storage (MySQL)**
- Cleaned and normalized data is stored in MySQL using a well-structured, normalized table design.
- Tables are capable of maintaining historical data for audit and trend analysis.

### 🔹 5. **Transformation (PySpark)**
- Initializes a Spark session, defines a strict schema, and loads the daily CSV dataset.
- Converts the Spark DataFrame to a **pandas-on-Spark** DataFrame for easier row-wise transformations.
- Key features engineered:
  - Parses `genre` strings into lists.
  - Extracts `release_year`, `decade`, and labels classics (released before 2000).
  - Adds a `blockbuster` flag for:
    - IMDb (vote count ≥ 10,000)
    - TMDB (vote count ≥ 5,000)
- Uses **Spark SQL window functions** to:
  - Rank top 2 movies per `language`, `release_year`, and `genre` based on `rating`.
  - Calculate average ratings per decade.
  - Count movies per genre.
  - Compute a running average rating over the years.
- Final enriched DataFrame is exported as an Excel file compatible with Airflow’s file system.

### 🔹 7. **Files Format**
- The files are bundled together and placed in a folder named after the processed date. This makes it easy to maintain and access specific files for a given date.
- A separate folder is created to store the latest Excel sheet, which serves as the source for the Power BI dashboard.

### 🔹 8. **Export for BI**
- Transformed PySpark DataFrame is exported as an **Excel** file using `pandas-on-Spark`.
- Used directly as a data source in Power BI.

### 🔹 9. **Dashboard (Power BI)**
- **Pages**:
  - Main Dashboard
  - Analytics Dashboard
- **Visuals**:
  - Bar & pie charts, cards, drill-downs, tables, slicers, gauges, treemaps
- **Key Metrics**:
  - Top popular movies
  - Average rating trends over the years
  - Movie count trend by release month
  - Average normalized score
  - Count of blockbuster movies
  - Classics vs modern content
  - Movie count by language

---

## 🏂 Key Skills Demonstrated

- Designing Airflow DAGs for ETL scheduling
- REST API integration and error handling
- Batch data cleaning using Apache Beam
- Complex transformations with PySpark and window functions
- SQL schema normalization and joining logic
- Power BI data storytelling and KPI design

---

## 🏁 Final Output

- ✅ Cleaned and enriched movie dataset with a consistent schema  
- 📊 Interactive Power BI dashboard for genre, ratings, and source-level insights  
- 🐳 Fully Dockerized, reproducible, and ready-to-deploy setup

---
## 📞 Contact Me

- If you like my content or you want to suggest any changes, I would be happy to connect.
- To connect with my on linkedin, please click [here](https://www.linkedin.com/in/sharath-j-503382219/)

---

## 📊 PowerBI Dashboard

- Main Dashboard Snapshot
  
  ![image](https://github.com/user-attachments/assets/c0b92ea6-d2f9-420f-9759-21fea5241a0b)

- Analytics Dashboard Snapshot
  
  ![image](https://github.com/user-attachments/assets/c204589f-f91c-46b4-93c8-c0ed36bbb741)

---

**Explore my other projects**

- [Real Time Stock Data Streaming Pipeline project](https://github.com/Sharathjagadeesh/RealTimeStreamingProject.git)

---

## 🙋 Author

**Sharath**  
Data Engineer | Real-time Streaming Enthusiast  | ETL | Batch Processing
📫 [LinkedIn Profile]([https://linkedin.com/in/yourprofile](https://www.linkedin.com/in/sharath-j-503382219/))

---

# docker-airflow from puckel

## The below details could be helpful if you want to replicate this project by installing docker and the services used in this project.

[![Docker Hub](https://img.shields.io/badge/docker-ready-blue.svg)](https://hub.docker.com/r/puckel/docker-airflow/)
[![Docker Pulls](https://img.shields.io/docker/pulls/puckel/docker-airflow.svg)]()
[![Docker Stars](https://img.shields.io/docker/stars/puckel/docker-airflow.svg)]()

This repository contains **Dockerfile** of [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/)'s [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).

## Informations

* Based on Python (3.7-slim-buster) official Image [python:3.7-slim-buster](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

## Installation

Pull the image from the Docker repository.

    docker pull puckel/docker-airflow

## Build

Optionally install [Extra Airflow Packages](https://airflow.incubator.apache.org/installation.html#extra-package) and/or python dependencies at build time :

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" -t puckel/docker-airflow .
    docker build --rm --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

or combined

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

Don't forget to update the airflow images in the docker-compose files to puckel/docker-airflow:latest.

## Usage

By default, docker-airflow runs Airflow with **SequentialExecutor** :

    docker run -d -p 8080:8080 puckel/docker-airflow webserver

If you want to run another executor, use the other docker-compose.yml files provided in this repository.

For **LocalExecutor** :

    docker-compose -f docker-compose-LocalExecutor.yml up -d

For **CeleryExecutor** :

    docker-compose -f docker-compose-CeleryExecutor.yml up -d

NB : If you want to have DAGs example loaded (default=False), you've to set the following environment variable :

`LOAD_EX=n`

    docker run -d -p 8080:8080 -e LOAD_EX=y puckel/docker-airflow

If you want to use Ad hoc query, make sure you've configured connections:
Go to Admin -> Connections and Edit "postgres_default" set this values (equivalent to values in airflow.cfg/docker-compose*.yml) :
- Host : postgres
- Schema : airflow
- Login : airflow
- Password : airflow

For encrypted connection passwords (in Local or Celery Executor), you must have the same fernet_key. By default docker-airflow generates the fernet_key at startup, you have to set an environment variable in the docker-compose (ie: docker-compose-LocalExecutor.yml) file to set the same key accross containers. To generate a fernet_key :

    docker run puckel/docker-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)"

## Configuring Airflow

It's possible to set any configuration value for Airflow from environment variables, which are used over values from the airflow.cfg.

The general rule is the environment variable should be named `AIRFLOW__<section>__<key>`, for example `AIRFLOW__CORE__SQL_ALCHEMY_CONN` sets the `sql_alchemy_conn` config option in the `[core]` section.

Check out the [Airflow documentation](http://airflow.readthedocs.io/en/latest/howto/set-config.html#setting-configuration-options) for more details

You can also define connections via environment variables by prefixing them with `AIRFLOW_CONN_` - for example `AIRFLOW_CONN_POSTGRES_MASTER=postgres://user:password@localhost:5432/master` for a connection called "postgres_master". The value is parsed as a URI. This will work for hooks etc, but won't show up in the "Ad-hoc Query" section unless an (empty) connection is also created in the DB

## Custom Airflow plugins

Airflow allows for custom user-created plugins which are typically found in `${AIRFLOW_HOME}/plugins` folder. Documentation on plugins can be found [here](https://airflow.apache.org/plugins.html)

In order to incorporate plugins into your docker container
- Create the plugins folders `plugins/` with your custom plugins.
- Mount the folder as a volume by doing either of the following:
    - Include the folder as a volume in command-line `-v $(pwd)/plugins/:/usr/local/airflow/plugins`
    - Use docker-compose-LocalExecutor.yml or docker-compose-CeleryExecutor.yml which contains support for adding the plugins folder as a volume

## Install custom python package

- Create a file "requirements.txt" with the desired python modules
- Mount this file as a volume `-v $(pwd)/requirements.txt:/requirements.txt` (or add it as a volume in docker-compose file)
- The entrypoint.sh script execute the pip install command (with --user option)

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


## Scale the number of workers

Easy scaling using docker-compose:

    docker-compose -f docker-compose-CeleryExecutor.yml scale worker=5

This can be used to scale to a multi node setup using docker swarm.

## Running other airflow commands

If you want to run other airflow sub-commands, such as `list_dags` or `clear` you can do so like this:

    docker run --rm -ti puckel/docker-airflow airflow list_dags

or with your docker-compose set up like this:

    docker-compose -f docker-compose-CeleryExecutor.yml run --rm webserver airflow list_dags

You can also use this to run a bash shell or any other command in the same environment that airflow would be run in:

    docker run --rm -ti puckel/docker-airflow bash
    docker run --rm -ti puckel/docker-airflow ipython

# Simplified SQL database configuration using PostgreSQL

If the executor type is set to anything else than *SequentialExecutor* you'll need an SQL database.
Here is a list of PostgreSQL configuration variables and their default values. They're used to compute
the `AIRFLOW__CORE__SQL_ALCHEMY_CONN` and `AIRFLOW__CELERY__RESULT_BACKEND` variables when needed for you
if you don't provide them explicitly:

| Variable            | Default value |  Role                |
|---------------------|---------------|----------------------|
| `POSTGRES_HOST`     | `postgres`    | Database server host |
| `POSTGRES_PORT`     | `5432`        | Database server port |
| `POSTGRES_USER`     | `airflow`     | Database user        |
| `POSTGRES_PASSWORD` | `airflow`     | Database password    |
| `POSTGRES_DB`       | `airflow`     | Database name        |
| `POSTGRES_EXTRAS`   | empty         | Extras parameters    |

You can also use those variables to adapt your compose file to match an existing PostgreSQL instance managed elsewhere.

Please refer to the Airflow documentation to understand the use of extras parameters, for example in order to configure
a connection that uses TLS encryption.

Here's an important thing to consider:

> When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it following the standard syntax of DB connections,
> where extras are passed as parameters of the URI (note that all components of the URI should be URL-encoded).

Therefore you must provide extras parameters URL-encoded, starting with a leading `?`. For example:

    POSTGRES_EXTRAS="?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fcerts%2Fca-certificates.crt"

# Simplified Celery broker configuration using Redis

If the executor type is set to *CeleryExecutor* you'll need a Celery broker. Here is a list of Redis configuration variables
and their default values. They're used to compute the `AIRFLOW__CELERY__BROKER_URL` variable for you if you don't provide
it explicitly:

| Variable          | Default value | Role                           |
|-------------------|---------------|--------------------------------|
| `REDIS_PROTO`     | `redis://`    | Protocol                       |
| `REDIS_HOST`      | `redis`       | Redis server host              |
| `REDIS_PORT`      | `6379`        | Redis server port              |
| `REDIS_PASSWORD`  | empty         | If Redis is password protected |
| `REDIS_DBNUM`     | `1`           | Database number                |

---

# Thank you for reading!
