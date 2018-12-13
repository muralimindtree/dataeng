// Databricks notebook source
//DEFINE THE INPUT FILES
val businessFile = "dbfs:/mnt/dataingestiondemo/data/input/businesses_plus.csv"
val inspectionFile = "dbfs:/mnt/dataingestiondemo/data/input/inspections_plus.csv"
val violationFile = "dbfs:/mnt/dataingestiondemo/data/input/violations_plus.csv"

// COMMAND ----------

//LOAD THE DATA TO DATAFRAMES AND CREATE TEMP TABLES FOR EXPLORATION
val businessDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(businessFile)
val inspectionDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(inspectionFile)
val violationDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(violationFile)

businessDF.createOrReplaceTempView("business")
inspectionDF.createOrReplaceTempView("inspection")
violationDF.createOrReplaceTempView("violation")

display(businessDF)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Do some basic exploration of data
// MAGIC SELECT DISTINCT(city) FROM business
// MAGIC 
// MAGIC --Data quality issues !!

// COMMAND ----------

//Lets clean up the cities a bit - write some spark code if needed to achieve custom processing

//Create a User defined function
def cleanUpCity=(s: String) => {
val cityMap = Map(
  "S.F."->"SFO",
  "sf"->"SFO",
  "s.F. Ca"->"SFO",
  "SF`"->"SFO",
  "SO. SAN FRANCISCO"->"SFO",
  "San Franciscvo"->"SFO",
  "san Francisco"->"SFO",
  "San Francisco"->"SFO",
  "null"->"SFO",
  "San Francsico"->"SFO",
  "Sf"->"SFO",
  "SO.S.F."->"SFO",
  "San Francisco,"->"SFO",
  "CA"->"SFO",
  "san Francisco CA"->"SFO",
  "San Francisco, CA"->"SFO",
  "SF"->"SFO",
  "SF, CA"->"SFO",
  "SF CA  94133"->"SFO",
  "San francisco"->"SFO",
  "SF , CA"->"SFO",
  "San Francisco, Ca"->"SFO",
  "SAN FRANCICSO"->"SFO",
  "San Franciisco"->"SFO",
  "Sand Francisco"->"SFO",
  "SF."->"SFO",
  "SAN FRANCISCO"->"SFO",
  "san francisco"->"SFO",
  "San Franicisco"->"SFO",
  "S F"->"SFO",
  "OAKLAND" -> "OAKLAND", 
  "Oakland" -> "OAKLAND")
  
  if(cityMap.get(s).isDefined) {
    cityMap.get(s).get
  } else{
    s
  }
}

//register the UDF
sqlContext.udf.register("clean_city",cleanUpCity)

//clean up the data
val cleanedBusinessDF = spark.sql("""
  SELECT business_id,
  name,
  address,
  clean_city(city) city,
  postal_code,
  latitude,
  longitude,
  phone_number,
  TaxCode,
  business_certificate,
  application_date,
  owner_name,
  owner_address,
  owner_city,
  owner_state,
  owner_zip
  FROM business
  """)
cleanedBusinessDF.createOrReplaceTempView("business")
display(cleanedBusinessDF)

//cities are cleaned up
//download preview results or full results

// COMMAND ----------

//clean up the inspections - remove all records that do not have a score
val cleanedInspectionsDF = spark.sql("""
   SELECT business_id, Score, inspections_date, type FROM inspection WHERE Score is not null
""")

// COMMAND ----------

//write the cleaned files
val cleanedBusinessFile = "dbfs:/mnt/dataingestiondemo/data/cleansed/businesses.csv"
cleanedBusinessDF.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(cleanedBusinessFile)

val cleanedInspectionsFile = "dbfs:/mnt/dataingestiondemo/data/cleansed/inspections.csv"
cleanedInspectionsDF.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(cleanedInspectionsFile)

val cleanedViolationsFile = "dbfs:/mnt/dataingestiondemo/data/cleansed/violations.csv"
violationDF.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(cleanedViolationsFile)

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct(type) from inspection