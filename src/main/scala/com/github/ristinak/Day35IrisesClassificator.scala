package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{RFormula, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object Day35IrisesClassificator extends App {
  //https://archive.ics.uci.edu/ml/datasets/iris
  val spark = getSpark("irisesClassification")
  val filePath = "./src/resources/irises/iris.data"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.describe().show(false)
  df.show(5, false)

  //now that we have data loaded with default column names _c0 ... _c4
  //we need to create two new columns one would be features (which combines all 4 measurements into a Vector of Doubles)
  //and we need to convert the string labels into numeric labels(most likely 0,1,2) again doubles

  val myRFormula = new RFormula() //RFormula is a quicker way of creating needed column
    .setFormula("flower ~ . ")
  //    .setFormula("_c4 ~ . + _c0 + _c1 + _c2 + _c3")

  val ndf = df.withColumnRenamed("_c4", "flower")
  ndf.show(5, false)

  val fittedRF = myRFormula.fit(ndf)
  val preparedDF = fittedRF.transform(ndf)
  preparedDF.show(false)
  preparedDF.sample(0.1).show(false)

  val Array(train, test) = preparedDF.randomSplit(Array(0.8, 0.2)) //so 80 percent for training and 20 percent for testing

  import org.apache.spark.ml.classification.DecisionTreeClassifier //this Algorithm is like a game of yes/no questions
  //like the party game 21 questions
  //we could creat more models out of different classifiers
  val decTree = new DecisionTreeClassifier() //there are hyperparameters we could adjust but not for now
    .setLabelCol("label")
    .setFeaturesCol("features")

  val fittedModel = decTree.fit(train) //this is the hard work here of creating the model


  val testDF = fittedModel.transform(test) //here we get some results

  testDF.show(30,false) //we should have roughly 30 (since 20% of 150 is 30)


  //let's how to make a features column using  VectorAssembler
  val va = new VectorAssembler()
    .setInputCols(Array("_c0","_c1","_c2","_c3"))
    .setOutputCol("features") //default name is kind of ugly vecAssembler
  val tdf = va.transform(df)
  tdf.show(5, false)

  //let's convert our string label _c4 into a numerical value

  val labelIndexer = new StringIndexer().setInputCol("_c4").setOutputCol("label")
  val labelDF = labelIndexer.fit(tdf).transform(tdf)
  labelDF.show(5,false)

  val fittedModel2 = decTree.fit(labelDF) //create a new model but I used ALL of the data!!!
  //so using test dataframe is sort of useless because we alreday learned from the whole dataset, so chance of overfit is extremely

  val fittedDF = fittedModel2.transform(test)
  fittedDF.show(5, false)

  //bare minimum to make a prediction with some classifier model is to have a Vector[Double] column default name being features
  fittedModel2.transform(test.select("features")).show(5, false)

  def showAccuracy(df: DataFrame): Unit = {
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df) //in order for this to work we need label and prediction columns
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${(1.0 - accuracy)}")
  }
  showAccuracy(fittedDF)
  showAccuracy(testDF)

  ////so why is this a bit misleading this 100% accuracy ?

  //TODO fit Irises data set using split of 75% for training and 25% for testing from preparedDF
  //TODO use any other Classification besides Decision Tree (so LogisticRegression would be fine)
  //https://spark.apache.org/docs/3.2.2/ml-classification-regression.html
  //Check Accuracy on the testing set (you can also show accuracy on training set, but that should be 100% :) )

  println("\n***** Kristina's solution to the exercise: *****\n")

  val Array(trainingDF, testingDF) = preparedDF.randomSplit(Array(0.75, 0.25))
  val lrModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  val lrFittedModel = lrModel.fit(trainingDF)
  val lrFittedDF = lrFittedModel.transform(testingDF)
  lrFittedDF.select("flower", "features", "label", "prediction", "probability").show(false)
  showAccuracy(lrFittedDF)


}
