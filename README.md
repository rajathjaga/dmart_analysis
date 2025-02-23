# Dmart analysis using pyspark

## Project Overview

The task is to create a data pipeline using PySpark to integrate and analyze sales data from three different sources: product information, sales transactions, and customer details. The goal is to establish a connection with PySpark, load the datasets, perform data transformations, and answer a set of analytical questions.

- **Product Information** (`Product.csv`)
- **Sales Transactions** (`Sales.csv`)
- **Customer Details** (`Customer.csv`)

The goal is to:

- Establish a PySpark connection
- Load datasets into PySpark DataFrames
- Clean and transform the data
- Answer analytical questions using PySpark queries

---

## Project Structure

```
|-- data/
|   |-- Product.csv
|   |-- Sales.csv
|   |-- Customer.csv
|
|--script/
|   |-- dmart_analysis.py
|-- README.md
```

---

## Setup Instructions

### 1. Environment Setup

Ensure you have access to Databricks and a cluster is running. You can upload your CSV files to the Databricks FileStore:

```python
# Upload files using the Databricks UI (Data > Add Data)
```

Or use the CLI to upload data:

```bash
databricks fs cp ./data/Product.csv dbfs:/FileStore/data/Product.csv
```

---

## PySpark Workflow

### 1. Establish PySpark Connection

In Databricks, a Spark session is automatically created, so you can directly start using it:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sales Data Analysis").getOrCreate()
```

### 2. Load CSV Files

Load the datasets into DataFrames:

```python
df_products = spark.read.csv("dbfs:/FileStore/data/Product.csv", header=True, inferSchema=True)
df_sales = spark.read.csv("dbfs:/FileStore/data/Sales.csv", header=True, inferSchema=True)
df_customers = spark.read.csv("dbfs:/FileStore/data/Customer.csv", header=True, inferSchema=True)
```

### 3. Data Cleaning and Transformation

- **Rename Columns for Consistency:**

```python
from pyspark.sql.functions import col

def standardize_column_names(df):
    """
    Standardizes column names by:
    - Converting to lowercase
    - Replacing spaces with underscores
    - Removing special characters
    """
    new_columns = [col(c).alias(c.lower().strip().replace(" ", "_")) for c in df.columns]
    return df.select(*new_columns)

# Apply this function to all DataFrames
df_products = standardize_column_names(df_product)
df_sales = standardize_column_names(df_sales)
df_customers = standardize_column_names(df_customer)
```

- **Handle Missing Values:**

```python
df_sales = df_sales.na.fill({"quantity_sold": 0, "price": 0})
```

- **Join DataFrames:**

```python
df_sales_product = df_sales.join(df_products, on="product_id", how="left")

df_final = df_sales_product.join(df_customers, on="customer_id", how="left")
```

### 4. Data Analysis

#### Example Queries

- **Total Sales for Each Product Category:**

```python
from pyspark.sql.functions import col, sum

df_final.groupBy("category") \
    .agg(sum(col("quantity_sold") * col("price")).alias("total_sales")) \
    .orderBy(col("total_sales").desc()) \
    .show()
```

- **Unique Products Sold in Each Region:**

```python
from pyspark.sql.functions import countDistinct

df_final.groupBy("region") \
    .agg(countDistinct("product_id").alias("unique_products_sold")) \
    .show()
```

---

## Analytical Questions

1. What is the total sales for each product category?
2. Which customer has made the highest number of purchases?
3. What is the average discount given on sales across all products?
4. How many unique products were sold in each region?
5. What is the total profit generated in each state?
6. Which product sub-category has the highest sales?
7. What is the average age of customers in each segment?
8. How many orders were shipped in each shipping mode?
9. What is the total quantity of products sold in each city?
10. Which customer segment has the highest profit margin?

---

## Results

The results are displayed using `.show()`.

