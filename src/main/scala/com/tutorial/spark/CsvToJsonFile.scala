package com.tutorial.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, date_format, slice, split, to_date, when, year}
import org.apache.spark.{SparkConf, SparkContext}

object CsvToJsonFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils");

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkExample")
      .getOrCreate();

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    //load file csv to dataframe or spark.sparkContext.textFile(path) to load csv to rdd cause sparkcontext have parallelize data
    val df = spark.read.format("csv")
      .option("header",true)
      .load("input/data.csv");
    df.createOrReplaceTempView("data");

    val df1 = df
      .withColumn("Date of birth",when(to_date(col("Date of birth")).isNotNull,col("Date of birth")).otherwise(date_format(to_date(col("Date of birth"), "dd/MM/yyyy"), "yyyy-MM-dd")))
      .withColumn("Age",(year(current_date()) - year(col("Date of birth"))).cast("int") +1 )
      .withColumn("City" , slice(split(col("address"), ","),-1,1)(0))
      .distinct()

    df1.createOrReplaceTempView("data1");
    df1.show(false);

    df1.coalesce(1)
      //.select(struct(col("Full name"),col("Dateofbirth"),col("Address"),col("Age"),col("City")) as "data")
      .write
      //.option("header","true")
      .mode("overwrite")
      .json("output/jsonFile/");
  }
}
