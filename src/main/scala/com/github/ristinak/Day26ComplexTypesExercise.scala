package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, desc, size, split, trim}

object Day26ComplexTypesExercise extends App {
  println("Ch6: Complex Data Types Exercise")

  //TODO open 4th of august CSV from 2011
  //create a new dataframe with all the original columns
  //plus array of split description
  //plus length of said array (size)
  //filter by size of at least 3
  //withSelect add 3 more columns for the first 3 words in this dataframe
  //show top 10 results sorted by first word

  //so 5 new columns (filtered rows) sorted and then top 10 results

  val spark = getSpark("ComplexTypes")
  val filePath = "src/resources/retail-data/by-day/2011-08-04.csv"
  val df = readDataWithView(spark, filePath)

  val bigDF = df
    .withColumn("split_description", split(trim(col("Description")), " "))
    .withColumn("split_size", size(col("split_description")))
    .filter(col("split_size") >= 3)
    .selectExpr("*", "split_description[0] as first", "split_description[1] as second", "split_description[3] as third")

  bigDF.selectExpr("Description", "split_description", "split_size", "first", "second", "third")
    .orderBy("first", "second")
    .show(20, false)

}

