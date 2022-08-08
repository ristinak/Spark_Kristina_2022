package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{asc, col, desc, expr, size, split}

object Day26ExerciseArrays extends App {

  //TODO open 4th of august CSV from 2011
  //create a new dataframe with all the original columns
  //plus array of of split description
  //plus length of said array (size)
  //filter by size of at least 3
  //withSelect add 3 more columns for the first 3 words in this dataframe
  //show top 10 results sorted by first word

  //so 5 new columns (filtered rows) sorted and then top 10 results

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-08-04.csv"

  val df = readDataWithView(spark, filePath)

  df.withColumn("Description_Array", split(col("Description"), " "))
    .withColumn("Array_Length", size(col("Description_Array")))
    // .selectExpr("Description_Array", "Array_Length", "Description_Array[0] as 1st", "Description_Array[1] as 2nd","Description_Array[2] as 3rd")
    .selectExpr("*", "Description_Array[0] as 1st", "Description_Array[1] as 2nd","Description_Array[2] as 3rd")
    .where("Array_Length >= 3")
    .orderBy(desc("1st"))
    .show(10, truncate = false)

  df.withColumn("Description_Array", split(col("Description"), " "))
    .withColumn("Array_Length", size(col("Description_Array")))
    // .selectExpr("Description_Array", "Array_Length", "Description_Array[0] as 1st", "Description_Array[1] as 2nd","Description_Array[2] as 3rd")
//    .selectExpr("*", "Description_Array[0] as 1st", "Description_Array[1] as 2nd","Description_Array[2] as 3rd")
    //same result as above selectExpr because we just add 3 more columns to alreayd existing DataFrame
    .withColumn("First", expr("Description_Array[0]"))
    .withColumn("Second", expr("Description_Array[1]"))
    .withColumn("Third", expr("Description_Array[2]"))
    .where("Array_Length >= 3")
    .orderBy(desc("First"))
    .show(10, truncate = false)

  df.withColumn("Description_Array", split(col("Description"), " "))
    .withColumn("Array_Length", size(col("Description_Array")))
    .selectExpr("*", "Description_Array[0] as 1st", "Description_Array[1] as 2nd","Description_Array[2] as 3rd")
    .orderBy(asc("Array_length")) // i want to see the least words first
  .show(25, false)

}
