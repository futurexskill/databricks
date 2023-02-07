// Databricks notebook source
val diamondsDf = spark.read.format("csv").option("header", "true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")


// COMMAND ----------

diamondsDf.count()

// COMMAND ----------

diamondsDf.show()

// COMMAND ----------

val filteredDf = diamonds_df.filter(diamonds_df("cut") === "Ideal" && diamonds_df("price") > 1000)




// COMMAND ----------

filteredDf.count()

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC diamonds_df = spark.read.format("csv").option("header", "true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
// MAGIC 
// MAGIC filtered_df = diamonds_df.filter((diamonds_df["cut"] == "Ideal") & (diamonds_df["price"] > 1000))

// COMMAND ----------

// MAGIC %python
// MAGIC filtered_df.show()

// COMMAND ----------


from pyspark.sql.functions import avg


grouped_df = filtered_df.groupBy("clarity").agg(avg("price"))

// COMMAND ----------

import org.apache.spark.sql.functions.avg

val groupedDf = filteredDf.groupBy("clarity").agg(avg("price"))

// COMMAND ----------

groupedDf.show()

// COMMAND ----------

val orderedDf = groupedDf.orderBy(groupedDf("avg(price)").desc)


// COMMAND ----------

orderedDf.show()

// COMMAND ----------

val orderedDfNew = orderedDf.withColumnRenamed("avg(price)", "avg_price")


// COMMAND ----------

orderedDfNew.show()

// COMMAND ----------


