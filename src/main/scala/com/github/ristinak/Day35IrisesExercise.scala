package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

object Day35IrisesExercise extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/irises/iris.data"

  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  val flowerDF = df.withColumnRenamed("_c4", "irisType")
  flowerDF.show()

  flowerDF.describe().show() //check for any inconsistencies, maybe there are some outliers
  //maybe some missing data


  val myRFormula = new RFormula().setFormula("irisType ~ . ")

  val fittedRF = myRFormula.fit(flowerDF)

  val preparedDF = fittedRF.transform(flowerDF)
  preparedDF.show(false)

  val Array(train, test) = preparedDF.randomSplit(Array(0.75, 0.25))

  //lets check Random Forest
  val rfc = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")

  //this is where the model actually trains
  val fittedModel = rfc.fit(train)

  //and now we can use the fittedModel to make predictions
  val testDF = fittedModel.transform(test)

  testDF.show(50, false)

  /**
   * Wrapper function for MulticlassClassificationEvaluator
   * @param df
   * @param label
   * @param prediction
   * @param metric
   */
  def showAccuracy(df: DataFrame, label:String="label", prediction:String="prediction", metric:String="accuracy"): Unit = {
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(label)
      .setPredictionCol(prediction)
      .setMetricName(metric)
    val accuracy = evaluator.evaluate(df) //in order for this to work we need label and prediction columns
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${1.0 - accuracy}")
  }

  showAccuracy(testDF)

  val lr = new LogisticRegression()
    .setLabelCol("label")
    .setFeaturesCol("features")

  val fittedLr = lr.fit(train)

  val testLr = fittedLr.transform(test)

  showAccuracy(testLr)

  //what happens if we choose bad hyperparameters for our model?

  val dt = new DecisionTreeClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxDepth(1) //so I say my decision tree will only ask a single question
  //what will be the best possible accuracy of such a model?
  //remember we have 3 types of flowers 50 each

  val fittedDT = dt.fit(train)
  val testDT = fittedDT.transform(test)

  showAccuracy(testDT)
  //so depth 1 is not recommended unles you have two classes (binary classifier) and are confident one question(this means covering just one feature)
  //is enough
  println(fittedDT.toDebugString) //this should show the questions it asks


  //i could write my own depth tester, basically a loop to test maximum depth

  def decisionTreeTester(classifier:DecisionTreeClassifier, train: DataFrame, test: DataFrame, debug:Boolean=true):Unit = {
    val model = classifier.fit(train)
    if (debug) println(model.toDebugString) //show the questions
    val df = model.transform(test)
    showAccuracy(df)
  }

  println("this is our own pipeline tester")
  (1 to 5).foreach(n => {
    dt.setMaxDepth(n)
    decisionTreeTester(dt, train,test )})


  //lets see if we can print the decisions

  //we can make our own official pipeline to test different hyperparameters

  val stages = Array(dt) //for now it is just the decision tree that we test

  val pipeline = new Pipeline().setStages(stages)

  //now we need to define what parameters we will test

  //maxBins
  //In decision trees, continuous features are converted into categorical features and maxBins
  //determines how many bins should be created from continous features. More bins gives a
  //higher level of granularity. The value must be greater than or equal to 2 and greater than or
  //equal to the number of categories in any categorical feature in your dataset. The default is 32.

  val parameters = new ParamGridBuilder()
    .addGrid(dt.maxDepth, (1 to 5).toArray)
    .addGrid(dt.maxBins, Array(8,16,32,64)) //so we will test 5 * 4 different hyperparameter combinations
    .build()

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val trainValidation = new TrainValidationSplit()
    .setTrainRatio(0.75)
    .setEstimatorParamMaps(parameters)
    .setEstimator(pipeline)
    .setEvaluator(evaluator)

  val tvFitted = trainValidation.fit(train) //so here all the loops will run checking all parameters
  //checking accuracy

  val testAccuracy = evaluator.evaluate(tvFitted.transform(test))
  println(s"Our model is $testAccuracy accurate on test data set")

  //we have run our Paramater Grid but which are the best parameters

  //so we get out the best model
  val trainedPipeline = tvFitted.bestModel.asInstanceOf[PipelineModel]

  //we only had one stage - stage 0
  val ourBestDecisionTree = trainedPipeline.stages(0).asInstanceOf[DecisionTreeClassificationModel]

  println(ourBestDecisionTree.toDebugString)


}
