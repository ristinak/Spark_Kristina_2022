package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.feature.RFormula

object Day32RFormula extends App {
  println("CH25: RFormula")
  //https://spark.apache.org/docs/latest/ml-features.html#rformula

  val spark = getSpark("sparky")

  val dataset = spark.createDataFrame(Seq(
    (7, "US", 18, 1.0),
    (8, "CA", 12, 0.0),
    (9, "NZ", 15, 0.0),
    (10, "LV", 215, 0.0),
    (15, "LT", 515, 55.0),
  )).toDF("id", "country", "hour", "clicked")

  dataset.show()

  val formula = new RFormula()
  //we are saying we want the label be from clicked column
  //and country and hour columns to be used for features
    .setFormula("clicked ~ country + hour")
    .setFeaturesCol("MYfeatures") //default is features which is fine
    .setLabelCol("MYlabel") //default is label which is usually fine

  val output = formula.fit(dataset).transform(dataset)
  output
//    .select("features", "label")
    .show()

  //TODO load into dataframe from retail-data by-day December 1st
  //TODO create RFormula to use Country as label and only UnitPrice and Quantity as Features
  //TODO make sure they are numeric columns - we do not want one hot encoding here
  //you can leave column names at default

  //create output dataframe with the formula performing fit and transform

  //TODO BONUS try creating features from ALL columns in the Dec1st CSV except of course Country (using . syntax)
  //This should generate very sparse column of features because of one hot encoding
}
