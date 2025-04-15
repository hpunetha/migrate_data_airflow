# Airflow Data Migration

This repository contains a setup for Apache Airflow with Docker Compose, including example DAGs and scripts for task execution.
I have referred this [repo - Airflow Example Repository](https://github.com/franceoliver/airflow_example_repo) for base example and added upon it for additional migration scripts.

## Prerequisites

- Docker
- Docker Compose

## Repository Structure

```
migrate_data_airflow/
├── dags/
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── migrate.py
│   │   ├── s3_utils.py
│   │   ├── transform.py
│   ├── scripts/
│   │   ├── __init__.py
│   │   ├── custom_operator.py
│   │   ├── task_functions.py
│   ├── __init__.py
│   ├── advanced_dag.py
│   ├── dummy_test_dag.py
│   ├── example_dag_1.py
│   ├── example_dag_2.py
│   ├── example_dag_3.py
│   ├── migrate_dag.py
│   ├── migrate_large_table_in_batches_dag.py
├── logs/


```

## Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone <repository_url>
   cd migrate_data_airflow
   ```

2. **Build the Docker Images**

   ```bash
   chmod +x entrypoint.sh
   docker-compose build
   ```

3. **Start Airflow Services**

   ```bash
   docker-compose up -d
   ```

4. **Access the Airflow UI**

   Open your browser and go to `http://localhost:8080`. Log in with the default admin credentials (`admin` / `admin`).

## Running the DAGs

1. **Add Your DAGs**

   Ensure your DAG files are located in the `dags/` directory.

2. **Trigger a DAG**

   To manually trigger a DAG:
   - Go to the Airflow UI.
   - Navigate to the "DAGs" tab.
   - Turn on the DAG toggle for the DAG you want to trigger.
   - Click the "Trigger DAG" button next to the DAG.

3. **Monitor DAG Runs**

   Monitor the progress and status of your DAG runs in the Airflow UI. Logs for each task can be viewed by clicking on the task instance.

## Troubleshooting

### Logs

Check the logs for the webserver, scheduler, and other services if you encounter issues:

```bash
docker-compose logs webserver
docker-compose logs scheduler
```

## Infrastructure Setup

Run the following commands to set up the required containers:

### 1. Create a Docker Network
```sh
docker network create dbnetwork
```

### 2. Set Up MariaDB
```sh
docker pull mariadb:latest
docker run --detach --network dbnetwork --name mariadb -p 3310:3306 --env MARIADB_ROOT_PASSWORD=password mariadb:latest
```

### 3. Set Up MySQL
```sh
docker run --detach --network dbnetwork --name mysqldb -p 3306:3306 --env MYSQL_ROOT_PASSWORD=password mysql:latest
```

### 4. Set Up MinIO (S3-Compatible Storage)
```sh
docker run --detach -p 9000:9000 -p 9001:9001 --network dbnetwork --name minio -v D:\minio\data:/data -e "MINIO_ROOT_USER=root" -e "MINIO_ROOT_PASSWORD=password" quay.io/minio/minio server /data --console-address ":9001"
```

## Database Setup

### 1. Create the `testdb` Database
Connect to both MySQL and MariaDB using a database client like DBeaver, then execute the following SQL:

```sql
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;
```

### 2. Create Tables and Insert Sample Data

#### Employees Table
```sql
CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL,
    department VARCHAR(50),
    salary DECIMAL(10,2),
    joining_date DATE
);

INSERT INTO employees (name, age, department, salary, joining_date) VALUES
('Alice Johnson', 30, 'HR', 60000.00, '2020-06-15'),
('Bob Smith', 35, 'Finance', 75000.00, '2018-03-25'),
('Charlie Brown', 28, 'IT', 50000.00, '2022-08-10'),
('David Williams', 40, 'Sales', 85000.00, '2015-11-05'),
('Eve Adams', 25, 'Marketing', 55000.00, '2023-01-12');
```

#### Departments Table
```sql
CREATE TABLE departments (
    dept_id INT AUTO_INCREMENT PRIMARY KEY,
    dept_name VARCHAR(50) NOT NULL UNIQUE,
    location VARCHAR(100),
    manager VARCHAR(100)
);

INSERT INTO departments (dept_name, location, manager) VALUES
('HR', 'New York', 'Alice Johnson'),
('Finance', 'London', 'Robert Williams'),
('IT', 'San Francisco', 'Sophia Davis'),
('Sales', 'Chicago', 'James Brown'),
('Marketing', 'Los Angeles', 'Emma Wilson');
```

#### Salaries Table
```sql
CREATE TABLE salaries (
    salary_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT NOT NULL,
    base_salary DECIMAL(10,2) NOT NULL,
    bonus DECIMAL(10,2) DEFAULT 0.00,
    total_compensation DECIMAL(10,2) GENERATED ALWAYS AS (base_salary + bonus) STORED,
    effective_date DATE NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
);

INSERT INTO salaries (employee_id, base_salary, bonus, effective_date) VALUES
(1, 60000.00, 5000.00, '2023-01-01'),
(2, 75000.00, 7000.00, '2023-01-01'),
(3, 50000.00, 3000.00, '2023-01-01'),
(4, 85000.00, 10000.00, '2023-01-01'),
(5, 55000.00, 4000.00, '2023-01-01');
```

### 3. Create a bucket in minio
- Open the MinIO Console at `http://localhost:9001`.
- Login with user and password as specified in the MinIO setup. (default is `root` and `password`)
- Create a bucket named 'bucket'


## Running the Application

### 1. Build the Docker Image
```sh
docker compose build --no-cache
```

### 2. Run the Application
```sh
docker compose up -d
```

### 3. Connect the mysql, mariadb and minio containers to network of airflow
```sh
docker network connect dbnetwork "mysqldb"
docker network connect dbnetwork "mariadb"
docker network connect dbnetwork "minio"
```
replace (dbnetwork) with network used for airflow on running docker compose (if changed)

### 4. Run the migrate_data DAG or migrate_large_table_in_batches_dag
The config.json file contains the base config which is used for copying specific tables or specific transformations. Similiar can be provided while running the dag and if provided will override the config.json 

---



## Conclusion

This setup provides a basic environment for running and managing Airflow DAGs using Docker Compose. Customize the `dags/` directory and other configurations as needed for your use case. For more information on Airflow, visit the [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/).


