package com.github.ristinak

import org.apache.spark.sql.functions.{col, desc, lit}

object Day23DifferentDataTypes extends App {
  //Documentation for Dataset (which applies to DataFrames as well) methods and properties
  //https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html

  //Now this may feel a bit overwhelming but have no fear, the majority of these functions are ones
  //that you will find in SQL and analytics systems. All of these tools exist to achieve one purpose,
  //to transform rows of data in one format or structure to another. This might create more rows or
  //reduce the number of rows available. To begin, let’s read in the DataFrame that we’ll be using
  //for this analysis:

  println("Ch6: Working with Different Types of Data")
  val spark = SparkUtil.getSpark("BasicSpark")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //we let Spark determine schema
    .load(filePath)

  //useful to start our operations with to see schema and have temp View ready for plain SQL commands
  df.printSchema() //notice everything is nullable possible data might be missing
  df.createOrReplaceTempView("dfTable") //we could have multiple Temp Views in a single app

  //One thing you’ll see us do throughout this chapter is convert native types to Spark types. We do
  //this by using the first function that we introduce here, the lit function. This function converts a
  //type in another language to its corresponding Spark representation. Here’s how we can convert a
  //couple of different kinds of Scala and Python values to their respective Spark types

  df.select(col("description"), lit("five"), lit(5.0)).show(4) //so all rows got these 3 new columns

  //There’s no equivalent function necessary in SQL, so we can use the values directly:
  spark.sql("SELECT  5, 'five', 5.0 FROM dfTable").show(2)
  spark.sql("SELECT description, 5, 'five', 5.0 as Float5 FROM dfTable").show(6, truncate = false)

  //so select ALL rows which have InvoiceNo equal to 536365
  df.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo", "Description")
    .show(5, false)

  //WARNING
  //Scala has some particular semantics regarding the use of == and ===. In Spark, if you want to filter by
  //equality you should use === (equal) or =!= (not equal). You can also use the not function and the
  //equalTo method.

  df.where(col("InvoiceNo") === 536365)
    .select("InvoiceNo", "Description")
    .show(5, false)

  //Another option—and probably the cleanest—is to specify the predicate as an expression in a
  //string. This is valid for Python or Scala. Note that this also gives you access to another way of
  //expressing “does not equal”
  df.where("InvoiceNo = 536365")
    .show(5, false)

  //lastly we can use regular SQL again
  spark.sql("SELECT * FROM dfTable WHERE InvoiceNo = 536365").show(5,false)

  //you can mix and match sql and Spark API
  spark.sql("SELECT * FROM dfTable WHERE InvoiceNo = 536365")
    .select("InvoiceNo", "Description")
    .show(3, false)

  //same as previous mix of sql and spark
  spark.sql("SELECT InvoiceNo,Description FROM dfTable WHERE InvoiceNo = 536365")
    .show(3, false)

  // EXERCISE //

  //TODO is load 1st of March of 2011 into dataFrame
  val filePath1 = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df1 = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //we let Spark determine schema
    .load(filePath1)

  df.createOrReplaceTempView("dfTable1")

  //TODO get all purchases that have been made from Finland

  println("All purchases made from Finland:")
  df1.where(col("Country") === "Finland").show()

  //TODO sort by Unit Price and LIMIT 20

  println("The first 20 purchases made from Finland, sorted by UnitPrice:")
  df1.where(col("Country") === "Finland")
    .orderBy(col("UnitPrice").desc)
    .limit(20)
    .show()

  //TODO collect results into an Array of Rows
  // print these 20 Rows

  val arrFinlandPurchases = df1.where(col("Country") === "Finland")
    .orderBy(col("UnitPrice").desc)
    .limit(20)
    .collect()

  arrFinlandPurchases.foreach(println)

  //You can use either SQL or Spark or mis of syntax

}
