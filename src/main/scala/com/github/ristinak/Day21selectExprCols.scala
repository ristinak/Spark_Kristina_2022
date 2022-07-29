package com.github.ristinak

import org.apache.spark.sql.functions.{expr, lit}

object Day21selectExprCols extends App {
  println("Ch 5: selectExpr and Column operations")
  val spark = SparkUtil.getSpark("Sparky")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  //so automatic detection of schema
  val df = spark.read.format("json")
    .load(flightPath)

  df.show(3)

  //  val statDf = df.describe() //shows basic statistics on string and numeric columns
  //  statDf.show()
  df.describe().show() //when you do not need to save the statistics dataframe for further use

  // in Scala
  //so we select two columns
  df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME as ORIGIN").show(2)

  //This opens up the true power of Spark. We can treat selectExpr as a simple way to build up
  //complex expressions that create new DataFrames. In fact, we can add any valid non-aggregating
  //SQL statement, and as long as the columns resolve, it will be valid! Here’s a simple example that
  //adds a new column withinCountry to our DataFrame that specifies whether the destination and
  //origin are the same

  df.selectExpr(
    "*", // include all original columns //more useful when you only need some columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
    .show(5)

  //alternative use withColumn

  // in Scala so same as above selectExpr
  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .show(2)

  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .withColumn("BigCount", expr("count * 100 + 10000")) //silly count inflation
    .show(5)

  //how could we get the row(s) that represents withinCountry flights
  //we could check for withinCountry value being true
  //or we could do it directly

  df.where("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME").show()
  //should show only 1 row with US to US flights

  //With select expression, we can also specify aggregations over the entire DataFrame by taking
  //advantage of the functions that we have. These look just like what we have been showing so far

  df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show()

  //Converting to Spark Types (Literals)
  //Sometimes, we need to pass explicit values into Spark that are just a value (rather than a new
  //column). This might be a constant value or something we’ll need to compare to later on. The
  //way we do this is through literals. This is basically a translation from a given programming
  //language’s literal value to one that Spark understands. Literals are expressions and you can use
  //them in the same way:

  //so before show we have created a new dataframe with an extra column with all 42s
  df.select(expr("*"), lit(42).as("The Answer!")).show(5)

  //even shorter example with selectExpr - same as above
  df.selectExpr("*", "42 as TheAnswer").show(3)

  //Adding Columns
  //There’s also a more formal way of adding a new column to a DataFrame, and that’s by using the
  //withColumn method on our DataFrame. For example, let’s add a column that just adds the
  //number one as a column

  // in Scala
  df.withColumn("number33", lit(33)).show(2)

  //we can add more than one column
  df.withColumn("numberOne", lit(1))
    .withColumn("numberTen", lit(10))
    .show(3)

  //Notice that the withColumn function takes two arguments: the column name and the expression
  //that will create the value for that given row in the DataFrame. Interestingly, we can also rename
  //a column this way. The SQL syntax is the same as we had previously, so we can omit it in this
  //example:
  //VS: so actually we did create a copy of an existing column with a new name
  //so you would need to also drop the col
  df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).show(3)

  //Renaming Columns - proper method
  // in Scala
  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show(3)

  //Reserved Characters and Keywords
  //One thing that you might come across is reserved characters like spaces or dashes in column
  //names. Handling these means escaping column names appropriately. In Spark, we do this by
  //using backtick (`) characters. Let’s use withColumn, which you just learned about to create a
  //column with reserved characters. We’ll show two examples—in the one shown here, we don’t
  //need escape characters, but in the next one, we do

  // ` - backtick symbol is usually on the upper left corner on m computer it is below Esc on the same key as ~

  //We don’t need escape characters here because the first argument to withColumn is just a string
  //for the new column name.
  val dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))
  dfWithLongColName.show(3)

  //Here we need to use backticks because we’re
  //referencing a column in an expression
  dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")
    .show(2)
}

