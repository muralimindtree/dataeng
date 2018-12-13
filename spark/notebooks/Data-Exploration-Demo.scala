// Databricks notebook source
//DEFINE THE INPUT FILES
val businessFile = "dbfs:/mnt/dataingestiondemo/data/cleansed/businesses.csv"
val inspectionFile = "dbfs:/mnt/dataingestiondemo/data/cleansed/inspections.csv"
val violationFile = "dbfs:/mnt/dataingestiondemo/data/cleansed/violations.csv"

// COMMAND ----------

//LOAD THE DATA TO DATAFRAMES AND CREATE TEMP TABLES FOR EXPLORATION
val businessDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(businessFile)
val inspectionDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(inspectionFile)
val violationDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(violationFile)

businessDF.createOrReplaceTempView("business")
inspectionDF.createOrReplaceTempView("inspection")
violationDF.createOrReplaceTempView("violation")

//Look at the schema from output

// COMMAND ----------

val fullJoinedDF = spark.sql("""
SELECT a.business_id as Business_ID, a.name AS Business_Name, a.city AS City, CONCAT(a.address, ', City: ', a.city, ', Zip: ', a.postal_code) AS Full_Address,
b.Score as Score, b.inspections_date as Event_Date, b.type as Inspection_Type,
c.date as Violation_Date, c.ViolationTypeID as ViolationType, c.risk_category as Risk, c.description as Violation_Detail
FROM business a INNER JOIN inspection b INNER JOIN violation c ON
a.business_id = b.business_id AND 
b.business_id = c.business_id
""")

// COMMAND ----------

val etlOutputFile = "dbfs:/mnt/dataingestiondemo/data/etl/business_inspection_violation.csv"
fullJoinedDF.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(etlOutputFile)