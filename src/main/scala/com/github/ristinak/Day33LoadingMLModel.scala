package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.tuning.TrainValidationSplitModel

object Day33LoadingMLModel extends App {
  println("Ch24: Loading Model")

  val spark = getSpark("Sparky")

  //After writing out the model, we can load it into another Spark program to make predictions. To
  //do this, we need to use a “model” version of our particular algorithm to load our persisted model
  //from disk. If we were to use CrossValidator, we’d have to read in the persisted version as the
  //CrossValidatorModel, and if we were to use LogisticRegression manually we would have
  //to use LogisticRegressionModel. In this case, we use TrainValidationSplit, which outputs
  //TrainValidationSplitModel:

  val modelPath = "src/resources/tmp/modelLocation"
  //remember we do not have this model in git you need to save it first in previous app
  val model = TrainValidationSplitModel.load(modelPath)

  var df = spark.read.json("src/resources/simple-ml")
  //here we are using data we actually used to train it but in real world we would have new data to transform

  val transformedDF = model.transform(df)

  transformedDF.show(10, false)


}
