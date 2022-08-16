package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.RFormula

object Day32Exercise extends App {

  //TODO load into dataframe from retail-data by-day December 1st
  // create RFormula to use Country as label and only UnitPrice and Quantity as Features
  // make sure they are numeric columns - we do not want one hot encoding here
  // you can leave column names at default
  // create output dataframe with the formula performing fit and transform

  val spark = getSpark("Sparky")
  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"
  val df = readDataWithView(spark, filePath)


  val formula = new RFormula()
    .setFormula("Country ~ UnitPrice + Quantity")

  val output = formula.fit(df).transform(df)
  output.show(20)


  //TODO BONUS try creating features from ALL columns in the Dec1st CSV except of course Country (using . syntax)
  //This should generate very sparse column of features because of one hot encoding

  val newFormula = new RFormula()
    .setFormula("Country ~ .")

  val newOutput = newFormula.fit(df).transform(df)
  newOutput.show(20, truncate = false)
//  newOutput.select("Country", "features", "label").show(20, truncate = false)

}
