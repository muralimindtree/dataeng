// Databricks notebook source
//LOAD ETL OUTPUT
import org.apache.spark.sql.functions._
val etlFile = "dbfs:/mnt/dataingestiondemo/data/etl/business_inspection_violation.csv"
val etlDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(etlFile)
etlDF.createOrReplaceTempView("etl")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- DO some visualization
// MAGIC -- QUESTION: FIND AVERAGE SCORE OF EACH BUSINESS
// MAGIC 
// MAGIC SELECT a.Business_Name AS Business, AVG(a.Score) AS AVG_SCORE FROM etl a
// MAGIC GROUP BY a.Business_Name
// MAGIC ORDER BY AVG_SCORE ASC

// COMMAND ----------

//REPORT #1
//FIND CITIES THAT HAVE HIGHEST VIOLATIONS

val violationsByCityDF = spark.sql("""
SELECT a.City, count(*) AS num_violations FROM etl a
WHERE a.Score <90 
GROUP BY a.City
ORDER BY num_violations DESC
""")

val violationsByCityFile = "dbfs:/mnt/dataingestiondemo/data/reports/violations_by_city.csv"
violationsByCityDF.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(violationsByCityFile)

// COMMAND ----------

//Write some Cubing queries
val businessScoresByYear = spark.sql("""
SELECT a.Business_Name AS name, substring(a.Violation_Date, 1,4) AS violation_year, a.Score from etl a
""")
businessScoresByYear.createOrReplaceTempView("business_scores_by_year")

val averageScoresDF = spark.sql("""
 SELECT name, violation_year, avg(Score) AS AVG_SCORE FROM 
business_scores_by_year
GROUP BY name, violation_year
ORDER BY name, violation_year
 """)


val cubedDF = averageScoresDF.cube("name", "violation_year").agg(avg("AVG_SCORE").alias("YEARLY_AVG"))
   .sort($"name".desc_nulls_last, $"violation_year".asc_nulls_last)

val avgScoresByBusinessFile = "dbfs:/mnt/dataingestiondemo/data/reports/scores_by_business_year.csv"
cubedDF.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(avgScoresByBusinessFile)