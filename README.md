# **Scala Spark Project Pipeline**

This repository demonstrates various ETL pipelines and data analysis use cases using **Scala Spark**. Each branch focuses on a specific scenario, showcasing different aspects of data processing and analysis.

---

## **Table of Contents**
1. [Project Overview](#project-overview)
2. [Available Branches](#available-branches)
3. [Project Structure](#project-structure)
4. [Technologies Used](#technologies-used)
5. [Setup and Installation](#setup-and-installation)
6. [Execution](#execution)
7. [Code Workflow](#code-workflow)
8. [License](#license)
9. [Contact](#contact)

---

## **Project Overview**

This project encompasses multiple use cases, including:
- Reading input data (e.g., Clients and Orders).
- Performing transformations such as joins, aggregations, and filtering.
- Writing the output in various formats like **Parquet**.
- Integrating Docker for environment consistency.
- Analyzing client orders through different analytical approaches.

---

## **Available Branches**

The repository contains multiple branches, each dedicated to a specific use case:

| Branch Name                                 | Description                                                  |
|---------------------------------------------|--------------------------------------------------------------|
| **features/UC_Clients_Orders_Docker**       | ETL pipeline for Clients & Orders with Docker integration.   |
| **features/UC_Analyse_des_Commandes_Clients_ud04** | Analysis use case (v4) of client orders.                     |
| **features/UC_Analyse_des_Commandes_Clients_ud03** | Analysis use case (v3) of client orders.                     |
| **features/UC_Analyse_des_Commandes_Clients_ud02** | Analysis use case (v2) of client orders.                     |

### **How to Switch Branches**

Use the following command to switch between branches:

```bash
git checkout <branch_name>
```

Example:

```bash
git checkout features/UC_Clients_Orders_Docker
```

---

## **Project Structure**

Each branch may have a slightly different structure based on its specific use case. However, a typical structure includes:

```plaintext
root_project/
│
├── job/                # Main Job Execution
├── utility/            # Reusable utility functions
├── mapping/            # Data transformations
├── common/             # Fetch and Runner logic
└── session/            # SparkSession Initialization
```

### **Detailed Overview**
- **job**: Contains the entry point to run the pipeline (e.g., `MainApp.scala`).
- **utility**: Holds utility functions such as file readers, loggers, and error handlers.
- **mapping**: Implements the transformations (e.g., joins, filtering, and aggregations).
- **common**: Logic to fetch raw data and manage the main execution.
- **session**: Sets up and initializes the SparkSession.

---

## **Technologies Used**
- **Scala**: Primary programming language.
- **Apache Spark**: Distributed data processing engine.
- **Maven**: Build and dependency management tool.
- **Parquet/CSV**: Input and output data formats.
- **Docker**: Containerization for consistent environments.

---

## **Setup and Installation**

1. **Clone the repository**:

   ```bash
   git clone https://github.com/WyTaSoft/training_spark.git
   cd training_spark
   ```

2. **Checkout the desired branch**:

   ```bash
   git checkout <branch_name>
   ```

3. **Build the project** using Maven:

   ```bash
   mvn clean package
   ```

4. **Docker Setup** (For branches with Docker integration):

   Ensure Docker and Docker Compose are installed. Then, run:

   ```bash
   docker-compose up --build
   ```

5. **Spark Configuration**: Ensure Spark is installed and properly configured if not using Docker.

---

## **Execution**

### **Run with Spark Submit**

For branches without Docker integration:

```bash
spark-submit \
    --class com.example.job.MainApp \
    --master local[*] \
    target/training_spark.jar
```

### **Dockerized Execution**

For Dockerized branches (e.g., `features/UC_Clients_Orders_Docker`):

```bash
docker-compose up --build
```

---

## **Code Workflow**

1. **SparkSession Initialization**:
   The session is initialized via the `session` package.

   ```scala
   val spark = SparkSession.builder()
     .appName("ScalaSparkPipeline")
     .master("local[*]")
     .getOrCreate()
   ```

2. **Read Data**:
   Use the **utility** functions to read Clients and Orders data.

   ```scala
   val clientsDF = Utility.readCSV(spark, "data/clients.csv")
   val ordersDF = Utility.readCSV(spark, "data/orders.csv")
   ```

3. **Transform Data**:
   Apply transformations (e.g., join and aggregate) via the **mapping** package.

   ```scala
   val transformedDF = Mapping.joinAndAggregate(clientsDF, ordersDF)
   ```

4. **Write Output**:
   Write the output in Parquet format.

   ```scala
   transformedDF.write.mode("overwrite").parquet("data/output/processed_orders")
   ```

---

## **License**
This project is licensed under the MIT License.

---

## **Contact**
For any questions, please reach out:
**Mehdi Tajmouati**  
[Website](https://www.wytasoft.com/wytasoft-group/) | [LinkedIn](https://www.linkedin.com/in/mtajmouati/) | [Medium](https://medium.com/@mehdi.tajmouati.wytasoft) | [Udemy](https://www.udemy.com/course/apache-spark-expertise-avancee/) | [Discord](https://discord.gg/NMtBzKFZ)
