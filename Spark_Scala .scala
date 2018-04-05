// Databricks notebook source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val spark = SparkSession.builder().getOrCreate()


// COMMAND ----------

val df = spark.read.option("header","true").option("InferSchema","true").csv("/FileStore/tables/Netflix_2011_2016.csv")

// COMMAND ----------

df.show()

// COMMAND ----------

df.columns

// COMMAND ----------

df.printSchema()

// COMMAND ----------

//df.head(5) or

for (row <- df.head(5))
   print (row)

// COMMAND ----------

df.describe().show()

// COMMAND ----------



val df2 = df.withColumn("HV",df("High")/df("volume"))
df2.select("HV").show()

// COMMAND ----------

df.select(mean("Close")).show()

// COMMAND ----------

df.select(min("volume")).show()

// COMMAND ----------

df.filter($"Volume" === 315541800).show()

// COMMAND ----------

df.select(max("volume")).show()

// COMMAND ----------

//what percentage of volumne is higher than 500.

(df.filter($"High" > 500).count() *1.0 /df.count )* 100 

// COMMAND ----------

// what is max high per year
val df3 = df.withColumn("year",year(df("Date")))
val max_year =df3.select("year","High").groupBy("year").max()
max_year.orderBy("year").show()

// COMMAND ----------


package com.hmovielabs.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

object PrintTweets {
 
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    statuses.print()
    
    ssc.start()
    ssc.awaitTermination()
  }  
}


// COMMAND ----------


