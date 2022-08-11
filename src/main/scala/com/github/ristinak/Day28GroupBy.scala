package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{count, desc, expr}

object Day28GroupBy extends App {
  println("Ch7: Grouping with Expressions")
  val spark = getSpark("Sparky")

  val df = readDataWithView(spark, "src/resources/retail-data/by-day/2010-12-01.csv")

  //Grouping
  //Thus far, we have performed only DataFrame-level aggregations. A more common task is to
  //perform calculations based on groups in the data. This is typically done on categorical data for
  //which we group our data on one column and perform some calculations on the other columns
  //that end up in that group.
  //The best way to explain this is to begin performing some groupings. The first will be a count,
  //just as we did before. We will group by each unique invoice number and get the count of items
  //on that invoice. Note that this returns another DataFrame and is lazily performed.
  //We do this grouping in two phases. First we specify the column(s) on which we would like to
  //group, and then we specify the aggregation(s). The first step returns a
  //RelationalGroupedDataset, and the second step returns a DataFrame.
  //As mentioned, we can specify any number of columns on which we want to group:

  df.groupBy("InvoiceNo", "CustomerId")
    .count()
    .orderBy(desc("count"))
    .show(10)
  //so we group by unique combination of the above columns and count the occurrences

  //same in SQL
  spark.sql(
    """
      |SELECT InvoiceNo, CustomerId, count(*) AS cnt FROM dfTable
      | GROUP BY InvoiceNo, CustomerId
      | ORDER BY cnt DESC
      |""".stripMargin)
    .show(10)

  //of course grouping by just invoiceNo should produce same results since
  // it is highly unlikely that same invoice number would apply to 2 or more different customers
  df.groupBy("InvoiceNo")
    .count()
    .orderBy(desc("count"))
    .show(10)

  //Grouping with Expressions
  //As we saw earlier, counting is a bit of a special case because it exists as a method. For this,
  //usually we prefer to use the count function. Rather than passing that function as an expression
  //into a select statement, we specify it as within agg. This makes it possible for you to pass-in
  //arbitrary expressions that just need to have some aggregation specified. You can even do things
  //like alias a column after transforming it for later use in your data flow

  df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)"))
    .show()

  df.withColumn("total", expr("round(Quantity * UnitPrice, 4)"))
    .show(10)

  //now that we have this total we can group by say invoice and see how much was total for each invoice
  //first we add extra total column
  df.withColumn("total", expr("round(Quantity * UnitPrice, 4)"))
    .groupBy("InvoiceNo")
    .agg(expr("count(Quantity) as Qcount"),
      expr("count(total) as Tcount"), //should be same as Qcount
      expr("round(sum(total),2) as totalSales"),
      expr("round(avg(total),2) as avgSales"),
      expr("min(total) as lowestSale"),
      expr("max(total) as maxSale")
    )
    .orderBy(desc("totalSales"))
    .show(10, false)

  df.withColumn("total", expr("round(Quantity * UnitPrice, 4)"))
    .groupBy("Country")
    .agg(expr("count(Quantity) as Qcount"),
      expr("count(total) as Tcount"), //should be same as Qcount
      expr("round(sum(total),2) as totalSales"),
      expr("round(avg(total),2) as avgSales"),
      expr("min(total) as lowestSale"),
      expr("max(total) as maxSale"),
      expr("round(std(total),2) as saleSTD"),
      expr("first(CustomerId) as firstCust"),
      expr("last(CustomerId) as lastCust"),
      expr("count(CustomerId) as customersPerCountry"),
    )
    .orderBy(desc("totalSales"))
    .show(10, false)

  //TODO see if you can count distinct customers by country in the same aggregation above
  //you could do it separately as well
  df.withColumn("total", expr("round(Quantity * UnitPrice, 4)"))
    .groupBy("Country")
    .agg(expr("count(Quantity) as Qcount"),
      expr("count(total) as Tcount"), //should be same as Qcount
      expr("round(sum(total),2) as totalSales"),
      expr("round(avg(total),2) as avgSales"),
      expr("min(total) as lowestSale"),
      expr("max(total) as maxSale"),
      expr("round(std(total),2) as saleSTD"),
      expr("first(CustomerId) as firstCust"),
      expr("last(CustomerId) as lastCust"),
      expr("count(CustomerId) as customersPerCountry"),
      expr("count(distinct(CustomerID)) as numOfDistinctCust")
    )
    .orderBy(desc("totalSales"))
    .show(10, false)


  //here is a full list of aggregation functions you can use
  //https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html
}
