package com.github.ristinak

import org.apache.parquet.format.IntType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, column, expr}

object Day21RowsDataFrameTransformations extends App {
  println("Chapter 5. Basic Structured Operations - Rows, DataFrame Transformations ")
  val spark = SparkUtil.getSpark("BasicSpark")

  //Creating Rows
  //You can create rows by manually instantiating a Row object with the values that belong in each
  //column. It’s important to note that only DataFrames have schemas. Rows themselves do not have
  //schemas. This means that if you create a Row manually, you must specify the values in the same
  //order as the schema of the DataFrame to which they might be appended

  val myRow = Row("Hello Sparky!", null, 555, false, 3.1415926)

  //we get access to individual members of Row, quite similar to how we would get access to individual members of an Array
//  println(myRow(0))
//  println(myRow(0).asInstanceOf[String]) // String
//  println(myRow.getString(0)) // String
//  val myGreeting = myRow.getString(0) //myGreeting is just normal Scala string
//  println(myGreeting)
//  println(myRow.getInt(2)) // Int
//  val myDouble = myRow.getInt(2).toDouble //we get and cast our Int as Double from Scala not Spark
//  println(myDouble)
//  val myPi = myRow.getDouble(4) //so 5th element
//  println(myPi)

  //so we can in fact print schema property for a single row
  //but we do not have printSchema method
//  println(myRow.schema) //so no schema unless Row comes from a DataFrame

  //DataFrame Transformations

  //When working with individual DataFrames there are some fundamental objectives.
  //These break down into several core operations

  //We can add rows or columns
  //We can remove rows or columns
  //We can transform a row into a column (or vice versa) - transposition
  //We can change the order of rows based on the values in column

  //the most common being those
  //that take one column, change it row by row, and then return our results

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  //so automatic detection of schema
  val df = spark.read.format("json")
    .load(flightPath)

  df.createOrReplaceTempView("dfTable") //view (virtual Table) needed to make SQL queries

//  df.show(5)

  //We can also create DataFrames on the fly by taking a set of rows and converting them to a
  //DataFrame.
  //we will need to define a schema
  val myManualSchema = new StructType(Array(
    StructField("some", StringType, true), //so true refers to this field/column being nullable namely could have null
    StructField("col", StringType, true),
    StructField("names", LongType, false))) //so names have to be present it is not nullable - ie required

  val myRows = Seq(Row("Hello", null, 1L), //we need to specify 1L because 1 by itself is an integer
    Row("Sparky", "some string", 151L),
    Row("Valdis", "my data", 9000L),
    Row(null, "my data", 151L)
  )

  //I do need to drop down to lower level RDD structure
  val myRDD = spark.sparkContext.parallelize(myRows) //you could add multiple partitions(numSlices) here which is silly when you only have 4 rows o data
//  val myDf = spark.createDataFrame(myRDD, myManualSchema) //so i pass RDD and schema and I get DataFrame
//  myDf.show()

  //there is a slightly hacky way to do conversions of Rows to DataFrames
  //NOTE
  //In Scala, we can also take advantage of Spark’s implicits in the console (and if you import them in
  //your JAR code) by running toDF on a Seq type. This does not play well with null types, so it’s not
  //necessarily recommended for production use cases.
  // in Scala
  //this requires implicits which are slowly going away in future versions
  //  val alsoDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

  //let’s take a look at their most useful methods that
  //you’re going to be using: the select method when you’re working with columns or expressions,
  //and the selectExpr method when you’re working with expressions in strings. Naturally some
  //transformations are not specified as methods on columns; therefore, there exists a group of
  //functions found in the org.apache.spark.sql.functions package.
  //With these three tools, you should be able to solve the vast majority of transformation challenges
  //that you might encounter in DataFrames.

  //select and selectExpr
  //select and selectExpr allow you to do the DataFrame equivalent of SQL queries on a table of
  //data:

  //In the simplest possible terms, you can use them to manipulate columns in your DataFrames.

  // in Scala
//  df.select("DEST_COUNTRY_NAME").show(2)

  //You can select multiple columns by using the same style of query, just add more column name
  //strings to your select method call

//  val newDF = df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
//  //again newDF is not holding the data in memory of our application
//  newDF.show(3)

//  println("Same thing using SQL syntax")
//  //with SQL syntax it is a bit harder to debug errors / typos etc
//  val sqlWay = spark.sql("""
//      SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME as my_ORIGIN
//      FROM dfTable
//      LIMIT 10
//      """)
//  sqlWay.show(3)

  //you can refer to columns in a number of different
  //ways; all you need to keep in mind is that you can use them interchangeably
  //try to stick with just one type in a single project
//  df.select(
//    df.col("DEST_COUNTRY_NAME"),
//    col("DEST_COUNTRY_NAME"),
//    column("DEST_COUNTRY_NAME"),
//    //    'DEST_COUNTRY_NAME, //not used as much required implicits which are being depreceated
//    //    $"DEST_COUNTRY_NAME",
//    expr("DEST_COUNTRY_NAME")) //expression lets us do more transformations than col or column
//    .show(2)

  //One common error is attempting to mix Column objects and strings. For example, the following
  //code will result in a compiler error:
  //df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME")

  //expr is the most flexible reference that we can use. It can refer to a plain
  //column or a string manipulation of a column. To illustrate, let’s change the column name, and
  //then change it back by using the AS keyword and then the alias method on the column:

  // in Scala
//  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

  //we can us alias instead to change the column name
  // in Scala
//  df.select(expr("DEST_COUNTRY_NAME as destination").alias("my destination"))
//    .show(2)


  //TODO create 3 Rows with the following data formats, string - holding food name, int - for holding quantity,
  // long for holding price
  // also boolean for holding isIt Vegan or not - so 4 data cells in each row
  // you will need to manually create a Schema - column names thus will be food, qty, price, isVegan
  // you  might need to import an extra type or two
  // import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType }

  val foodSchema = new StructType(Array(
    StructField("food", StringType, false), //so true refers to this field/column being nullable namely could have null
    StructField("qty", IntegerType, false),
    StructField("price", DoubleType, false),
    StructField("isVegan", BooleanType, true))) //so names have to be present it is not nullable - ie required

  //TODO create a dataFrame called foodFrame which will hold those Rows

  val foodRows = Seq(
    Row("Cottage cheese", 1, 3.49, false), //we need to specify 1L because 1 by itself is an integer
    Row("Sourdough bread", 2, 4.99, true),
    Row("Dumplings", 10, 1.89, null),
    Row("Smoked salmon", 2, 5.69, false)
  )

  val foodRDD = spark.sparkContext.parallelize(foodRows) //you could add multiple partitions(numSlices) here which is silly when you only have 4 rows o data
  val foodFrame = spark.createDataFrame(foodRDD, foodSchema)

  // Use Select or/an SQL syntax to select and show only name and qty

  foodFrame.select(col("food"), col("qty")).show()

  //so 3 Rows of food each Row has 4 entries (columns) so original data could be something like Chocolate, 3, 2.49, false
  //in Schema , name, qty, price are required (not nullable) while isVegan could be null
}

