# 🚀 Day 15 - Spark UDF Practice

## 📌 Overview

Day 15 focuses on **User Defined Functions (UDFs) in Apache Spark using Scala**.

The objective of this exercise is to understand how custom business logic can be applied to Spark DataFrame columns using UDFs, how UDFs can be registered with the Spark Catalog, and how registered UDFs can be used inside Spark SQL queries.

This project also demonstrates customer transaction risk classification using a custom UDF.

---

## 🎯 Objectives

The main objectives of Day 15 are:

- Understand Spark User Defined Functions (UDFs)
- Create Scala UDFs
- Apply UDFs to DataFrame columns
- Create calculated columns using UDFs
- Compare UDFs with built-in Spark functions
- Register UDFs with the Spark Catalog
- Use registered UDFs through Spark SQL
- Implement customer risk classification
- Generate risk summary reports
- Filter high-risk customers

---

## 🛠️ Technologies Used

- ☕ Java 8
- 🔥 Apache Spark 3.5.3
- 🐘 Scala 2.12.18
- 📦 SBT 1.12.11
- 🖥️ Ubuntu Linux
- 💻 IntelliJ IDEA / VS Code / Terminal

---

## 📂 Project Structure

```text
day15-spark/
│
├── .gitignore
├── build.sbt
├── README.md
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        └── scala/
            └── Day15UDFPractice.scala
```

---

## 📚 Concepts Covered

### 1️⃣ User Defined Functions

A UDF allows us to create custom logic that can be applied to Spark DataFrame columns.

Example:

```scala
val salaryBandUDF = udf((salary: Double) => {

  if (salary < 50000) {
    "Low"
  } else if (salary < 80000) {
    "Medium"
  } else if (salary < 120000) {
    "High"
  } else {
    "Very High"
  }

})
```

The UDF classifies employees based on their salary.

---

### 2️⃣ Salary Band Classification

The employee salary is classified into four categories:

| Salary Range | Salary Band |
|---|---|
| `< 50,000` | Low |
| `50,000 - < 80,000` | Medium |
| `80,000 - < 120,000` | High |
| `>= 120,000` | Very High |

The UDF is applied using:

```scala
withColumn(
  "salary_band",
  salaryBandUDF(col("salary"))
)
```

---

### 3️⃣ Annual Salary Calculation

A second UDF calculates annual salary from monthly salary.

```scala
val annualSalaryUDF = udf(
  (salary: Double) => salary * 12
)
```

The calculated value is added using:

```scala
.withColumn(
  "annual_salary",
  annualSalaryUDF(col("salary"))
)
```

---

## 🔄 UDF vs Built-in Spark Functions

The project compares custom UDF logic with Spark's built-in functions.

### UDF

```scala
val annualSalaryUDF = udf(
  (salary: Double) => salary * 12
)
```

### Built-in Spark Expression

```scala
val builtInSalary = employees.withColumn(
  "annual_salary",
  col("salary") * 12
)
```

### Key Difference

UDFs are useful when custom business logic is required and there is no suitable built-in Spark function.

When an equivalent built-in Spark function or expression exists, it is generally preferable because Spark can optimize built-in expressions more effectively.

---

## 📝 Registering a UDF

The project registers a salary classification UDF with the Spark Catalog.

```scala
spark.udf.register(
  "salary_band",
  (salary: Double) => {
    if (salary < 50000) {
      "Low"
    } else if (salary < 80000) {
      "Medium"
    } else if (salary < 120000) {
      "High"
    } else {
      "Very High"
    }
  }
)
```

After registration, the UDF can be used directly in Spark SQL.

---

## 🔎 Using UDF Through Spark SQL

The employee DataFrame is registered as a temporary SQL view:

```scala
employeeSalaryReport.createOrReplaceTempView("employees")
```

The registered UDF is then used in SQL:

```sql
SELECT
  employee_id,
  name,
  salary,
  salary_band(salary) AS salary_category
FROM employees
```

This demonstrates how a registered Scala UDF can be integrated with Spark SQL.

---

## 👥 Customer Transaction Risk Classification

The project also contains customer transaction data with:

- Customer ID
- Customer name
- Transaction count
- Total transaction value

A custom UDF is used to classify customers into risk categories.

### Risk Classification Rules

| Condition | Risk Category |
|---|---|
| Transaction count >= 20 OR total value >= 200,000 | High Risk |
| Transaction count >= 10 OR total value >= 75,000 | Medium Risk |
| Otherwise | Low Risk |

The UDF is applied using:

```scala
val customerRiskReport = customers.withColumn(
  "risk_category",
  customerRiskUDF(
    col("transaction_count"),
    col("total_transaction_value")
  )
)
```

---

## 📊 Risk Summary

The project generates a summary of customers by risk category.

```scala
customerRiskReport
  .groupBy("risk_category")
  .count()
  .orderBy(desc("count"))
  .show()
```

This helps understand the distribution of customers across different risk categories.

---

## 🚨 High Risk Customers

The project filters customers classified as high risk:

```scala
customerRiskReport
  .filter(col("risk_category") === "High Risk")
  .select(
    "customer_id",
    "name",
    "transaction_count",
    "total_transaction_value",
    "risk_category"
  )
  .show()
```

This demonstrates how UDF-generated columns can be used in further DataFrame transformations.

---

## ▶️ How to Run

### Step 1: Clone the Repository

```bash
git clone https://github.com/Harshita245-tech/day15-spark-udf.git
```

### Step 2: Navigate to the Project

```bash
cd day15-spark-udf
```

### Step 3: Compile the Project

```bash
sbt compile
```

### Step 4: Run the Application

```bash
sbt run
```

---

## ✅ Expected Program Sections

When the application runs successfully, the console displays:

```text
==============================================
DAY 15 - UDF PRACTICE
==============================================

===== EMPLOYEE DATA =====

===== SALARY BAND USING UDF =====

===== ANNUAL SALARY USING UDF =====

===== UDF VS BUILT-IN FUNCTION =====

===== REGISTERED UDF =====

===== SQL QUERY USING REGISTERED UDF =====

===== CUSTOMER TRANSACTION DATA =====

===== CUSTOMER RISK CATEGORY =====

===== CUSTOMER RISK SUMMARY =====

===== HIGH RISK CUSTOMERS =====

===== UDF EXPLANATION =====

==============================================
DAY 15 UDF PRACTICE COMPLETED
==============================================
```

---

## 💡 Key Learnings

Through this exercise, I learned:

- How to create UDFs in Scala
- How to apply custom logic to Spark DataFrame columns
- How to create calculated columns using UDFs
- How to register UDFs with Spark
- How to call registered UDFs from Spark SQL
- How to perform customer risk classification
- How to group and summarize UDF-generated categories
- Why built-in Spark functions are generally preferred when they provide equivalent functionality

---

## 🧠 Real-World Applications

UDFs can be useful for application-specific business rules such as:

- 💰 Salary classification
- 🚨 Customer risk classification
- 🏦 Financial transaction classification
- 📦 Product categorization
- 👤 Customer segmentation
- 📊 Business-specific scoring
- 🔍 Data quality rules
- 🛒 E-commerce customer classification

---

## 📌 Project Information

**Project:** Day 15 - Spark UDF Practice

**Repository:** `day15-spark-udf`

**Language:** Scala

**Framework:** Apache Spark

**Spark Version:** 3.5.3

**Scala Version:** 2.12.18

**Build Tool:** SBT

**Status:** ✅ Completed

---
