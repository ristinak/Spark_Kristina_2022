package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}

object Day32PipelineExample extends App {
  println("Ch24: Pipeline Example")
  //https://spark.apache.org/docs/latest/ml-pipeline.html
  val spark = getSpark("Sparky")
  var df = spark.read.json("src/resources/simple-ml")
  df.printSchema()
  // //Pipelining Our Workflow
  //  //As you probably noticed, if you are performing a lot of transformations, writing all the steps and
  //  //keeping track of DataFrames ends up being quite tedious. That’s why Spark includes the
  //  //Pipeline concept. A pipeline allows you to set up a dataflow of the relevant transformations
  //  //that ends with an estimator that is automatically tuned according to your specifications, resulting
  //  //in a tuned model ready for use.
  //
  //  //Note that it is essential that instances of transformers or models are not reused across different
  //  //pipelines. Always create a new instance of a model before creating another pipeline.
  //  //In order to make sure we don’t overfit, we are going to create a holdout test set and tune our
  //  //hyperparameters based on a validation set (note that we create this validation set based on the
  //  //original dataset, not the preparedDF used in the previous pages):
  //
  //  //original dataset, not the preparedDF used in the previous pages):
  //  // in Scala
  val Array(train, test) = df.randomSplit(Array(0.7, 0.3)) //often we use 80-20 split

  //we will use train to - well train the model
  train.describe().show()

  //holdout set we will use test to see how well we did - it is crucial that none of these test data points were used in training
  test.describe().show()

  //Now that you have a holdout set, let’s create the base stages in our pipeline. A stage simply
  //represents a transformer or an estimator. In our case, we will have two estimators. The RFomula
  //will first analyze our data to understand the types of input features and then transform them to
  //create new features. Subsequently, the LogisticRegression object is the algorithm that we will
  //train to produce a model:

  // in Scala
  val rForm = new RFormula()
  val lr = new LogisticRegression()

  //We will set the potential values for the RFormula in the next section. Now instead of manually
  //using our transformations and then tuning our model we just make them stages in the overall
  //pipeline, as in the following code snippet

  val stages = Array(rForm, lr)
  val pipeline = new Pipeline().setStages(stages)

  //so the big idea is that instead of writing multiple nested loops by hand and trying different combinations
  //we let mllib do it for us

  //Training and Evaluation
  //Now that you arranged the logical pipeline, the next step is training. In our case, we won’t train
  //just one model (like we did previously); we will train several variations of the model by
  //specifying different combinations of hyperparameters that we would like Spark to test. We will
  //then select the best model using an Evaluator that compares their predictions on our validation
  //data. We can test different hyperparameters in the entire pipeline, even in the RFormula that we
  //use to manipulate the raw data. This code shows how we go about doing tha

  val params = new ParamGridBuilder()
    .addGrid(rForm.formula, Array(
      "lab ~ . + color:value1",
      "lab ~ . + color:value1 + color:value2"))
    .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
    .addGrid(lr.regParam, Array(0.1, 2.0))
    .build()

  //in the above I created a grid of two possible feature vectors , then 3 different elasticNetParame, and two regularization parameters
  //so our grid will check 2*3*2 = 12 different combinations of these parameters

  //In our current paramter grid, there are three hyperparameters that will diverge from the defaults:
  //Two different versions of the RFormula
  //Three different options for the ElasticNet parameter
  //Two different options for the regularization parameter
  //This gives us a total of 12 different combinations of these parameters, which means we will be
  //training 12 different versions of logistic regression. We explain the ElasticNet parameter as
  //well as the regularization options in Chapter 26.
  //Now that the grid is built, it’s time to specify our evaluation process. The evaluator allows us to
  //automatically and objectively compare multiple models to the same evaluation metric. There are
  //evaluators for classification and regression, covered in later chapters, but in this case we will use
  //the BinaryClassificationEvaluator, which has a number of potential evaluation metrics, as
  //we’ll discuss in Chapter 26. In this case we will use areaUnderROC, which is the total area under
  //the receiver operating characteristic, a common measure of classification performance:

  val evaluator = new BinaryClassificationEvaluator()
    .setMetricName("areaUnderROC") //different Evaluators will have different metric options
    .setRawPredictionCol("prediction")
    .setLabelCol("label")

  //Now that we have a pipeline that specifies how our data should be transformed, we will perform
  //model selection to try out different hyperparameters in our logistic regression model and
  //measure success by comparing their performance using the areaUnderROC metric.
  //As we discussed, it is a best practice in machine learning to fit hyperparameters on a validation
  //set (instead of your test set) to prevent overfitting. For this reason, we cannot use our holdout test
  //set (that we created before) to tune these parameters. Luckily, Spark provides two options for
  //performing hyperparameter tuning automatically. We can use TrainValidationSplit, which
  //will simply perform an arbitrary random split of our data into two different groups, or
  //CrossValidator, which performs K-fold cross-validation by splitting the dataset into k non-
  //overlapping, randomly partitioned folds

  val tvs = new TrainValidationSplit()
    .setTrainRatio(0.75) // also the default.
    .setEstimatorParamMaps(params) //so this is grid of what different hyperparameters
    .setEstimator(pipeline) //these are the various tasks we want done /transformations /
    .setEvaluator(evaluator) //and this is the metric to judge our success

//  /Let’s run the entire pipeline we constructed. To review, running this pipeline will test out every
  //version of the model against the validation set. Note the type of tvsFitted is
  //TrainValidationSplitModel. Any time we fit a given model, it outputs a “model” type:

  // in Scala
  val tvsFitted = tvs.fit(train) //so this will actually do the work of fitting/making the best model

  //And of course evaluate how it performs on the test set!
  println("Test Evaluation", evaluator.evaluate(tvsFitted.transform(test)))

//We can also see a training summary for some models. To do this we extract it from the pipeline,
  //cast it to the proper type, and print our results. The metrics available on each model are discussed
  //throughout the next several chapters. Here’s how we can see the results:

  val trainedPipeline = tvsFitted.bestModel.asInstanceOf[PipelineModel]
  val TrainedLR = trainedPipeline.stages(1).asInstanceOf[LogisticRegressionModel]
  val summaryLR = TrainedLR.summary
  summaryLR.objectiveHistory
  //Persisting and Applying Models
  //Now that we trained this model, we can persist it to disk to use it for prediction purposes later on:
  tvsFitted.write.overwrite().save("src/resources/tmp/modelLocation")

  //After writing out the model, we can load it into another Spark program to make predictions. To
  //do this, we need to use a “model” version of our particular algorithm to load our persisted model
  //from disk.
}
