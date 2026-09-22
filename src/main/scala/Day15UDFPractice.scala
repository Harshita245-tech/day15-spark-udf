import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15UDFPractice {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // Spark Session
    // --------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day 15 - UDF Practice")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    println("==============================================")
    println("DAY 15 - UDF PRACTICE")
    println("==============================================")

    // --------------------------------------------------
    // 1. Employee Salary Dataset
    // --------------------------------------------------

    val employeeData = Seq(
      ("E001", "Aarav", "IT", 45000.0),
      ("E002", "Diya", "HR", 65000.0),
      ("E003", "Rahul", "Finance", 85000.0),
      ("E004", "Ananya", "IT", 120000.0),
      ("E005", "Arjun", "Sales", 55000.0),
      ("E006", "Meera", "Finance", 95000.0),
      ("E007", "Kiran", "IT", 150000.0),
      ("E008", "Sneha", "HR", 40000.0)
    )

    val employees = employeeData.toDF(
      "employee_id",
      "name",
      "department",
      "salary"
    )

    println("\n===== EMPLOYEE DATA =====")

    employees.show()

    // --------------------------------------------------
    // 2. Scala UDF - Salary Band
    // --------------------------------------------------

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

    println("\n===== SALARY BAND USING UDF =====")

    val salaryBandData = employees.withColumn(
      "salary_band",
      salaryBandUDF(col("salary"))
    )

    salaryBandData.show()

    // --------------------------------------------------
    // 3. Calculated Column Using UDF
    // --------------------------------------------------

    val annualSalaryUDF = udf((salary: Double) => salary * 12)

    println("\n===== ANNUAL SALARY USING UDF =====")

    val employeeSalaryReport = salaryBandData
      .withColumn(
        "annual_salary",
        annualSalaryUDF(col("salary"))
      )

    employeeSalaryReport.show()

    // --------------------------------------------------
    // 4. Compare UDF with Built-in Spark Functions
    // --------------------------------------------------

    println("\n===== UDF VS BUILT-IN FUNCTION =====")

    println("UDF:")
    println("- Custom salary classification logic")
    println("- Uses Scala code")
    println("- Useful for application-specific business logic")

    println("\nBuilt-in Spark Function:")
    println("- Uses Spark's optimized functions")
    println("- Usually preferred when an equivalent function exists")
    println("- Can benefit directly from Spark SQL optimization")

    val builtInSalary = employees.withColumn(
      "annual_salary",
      col("salary") * 12
    )

    println("\nAnnual salary using built-in Spark expression:")

    builtInSalary.show()

    // --------------------------------------------------
    // 5. Register UDF with Spark Catalog
    // --------------------------------------------------

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

    println("\n===== REGISTERED UDF =====")

    println("UDF 'salary_band' registered successfully.")

    // --------------------------------------------------
    // 6. Use Registered UDF Through SQL
    // --------------------------------------------------

    employeeSalaryReport.createOrReplaceTempView("employees")

    println("\n===== SQL QUERY USING REGISTERED UDF =====")

    spark.sql(
      """
        |SELECT
        |  employee_id,
        |  name,
        |  salary,
        |  salary_band(salary) AS salary_category
        |FROM employees
      """.stripMargin
    ).show()

    // --------------------------------------------------
    // 7. Customer Transaction Data
    // --------------------------------------------------

    val customerTransactions = Seq(
      ("C001", "Aarav", 5, 25000.0),
      ("C002", "Diya", 12, 85000.0),
      ("C003", "Rahul", 3, 12000.0),
      ("C004", "Ananya", 18, 150000.0),
      ("C005", "Arjun", 7, 45000.0),
      ("C006", "Meera", 15, 95000.0),
      ("C007", "Kiran", 25, 220000.0),
      ("C008", "Sneha", 2, 8000.0)
    )

    val customers = customerTransactions.toDF(
      "customer_id",
      "name",
      "transaction_count",
      "total_transaction_value"
    )

    println("\n===== CUSTOMER TRANSACTION DATA =====")

    customers.show()

    // --------------------------------------------------
    // 8. Customer Risk Category UDF
    // --------------------------------------------------

    val customerRiskUDF = udf(
      (transactionCount: Int, totalValue: Double) => {

        if (transactionCount >= 20 || totalValue >= 200000) {
          "High Risk"
        } else if (transactionCount >= 10 || totalValue >= 75000) {
          "Medium Risk"
        } else {
          "Low Risk"
        }

      }
    )

    println("\n===== CUSTOMER RISK CATEGORY =====")

    val customerRiskReport = customers.withColumn(
      "risk_category",
      customerRiskUDF(
        col("transaction_count"),
        col("total_transaction_value")
      )
    )

    customerRiskReport.show()

    // --------------------------------------------------
    // 9. Customer Risk Summary
    // --------------------------------------------------

    println("\n===== CUSTOMER RISK SUMMARY =====")

    customerRiskReport
      .groupBy("risk_category")
      .count()
      .orderBy(desc("count"))
      .show()

    // --------------------------------------------------
    // 10. High Risk Customers
    // --------------------------------------------------

    println("\n===== HIGH RISK CUSTOMERS =====")

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

    // --------------------------------------------------
    // 11. UDF Explanation
    // --------------------------------------------------

    println("\n===== UDF EXPLANATION =====")

    println("A User Defined Function allows custom logic")
    println("to be applied to Spark DataFrame columns.")

    println("In this project, UDFs are used for:")
    println("1. Salary band classification.")
    println("2. Annual salary calculation.")
    println("3. Customer risk classification.")

    println(
      "Built-in Spark functions should generally be preferred when they can perform the same operation."
    )

    // --------------------------------------------------
    // Completion
    // --------------------------------------------------

    println("\n==============================================")
    println("DAY 15 UDF PRACTICE COMPLETED")
    println("==============================================")

    spark.stop()
  }
}
