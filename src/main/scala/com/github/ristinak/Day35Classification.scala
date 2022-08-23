package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.linalg.Vectors

object Day35Classification extends App {
  println("Ch26: Classification")
  val spark = getSpark("sparky")

  //Chapter 26. Classification
  //Classification is the task of predicting a label, category, class, or discrete variable given some
  //input features. The key difference from other ML tasks, such as regression, is that the output
  //label has a finite set of possible values (e.g., three classes).
  //Use Cases
  //Classification has many use cases, as we discussed in Chapter 24. Here are a few more to
  //consider as a reinforcement of the multitude of ways classification can be used in the real world.
  //Predicting credit risk
  //A financing company might look at a number of variables before offering a loan to a
  //company or individual. Whether or not to offer the loan is a binary classification problem.
  //News classification
  //An algorithm might be trained to predict the topic of a news article (sports, politics, business,
  //etc.).
  //Classifying human activity
  //By collecting data from sensors such as a phone accelerometer or smart watch, you can
  //predict the person’s activity. The output will be one of a finite set of classes (e.g., walking,
  //sleeping, standing, or running).
  //Types of Classification
  //Before we continue, let’s review several different types of classification.
  //Binary Classification
  //The simplest example of classification is binary classification, where there are only two labels
  //you can predict. One example is fraud analytics, where a given transaction can be classified as
  //fraudulent or not; or email spam, where a given email can be classified as spam or not spam.
  //Multiclass Classification
  //Beyond binary classification lies multiclass classification, where one label is chosen from more
  //than two distinct possible labels. A typical example is Facebook predicting the people in a given
  //photo or a meterologist predicting the weather (rainy, sunny, cloudy, etc.). Note how there is
  //always a finite set of classes to predict; it’s never unbounded. This is also called multinomial
  //classification.
  //Multilabel Classification
  //Finally, there is multilabel classification, where a given input can produce multiple labels. For
  //example, you might want to predict a book’s genre based on the text of the book itself. While
  //this could be multiclass, it’s probably better suited for multilabel because a book may fall into
  //multiple genres. Another example of multilabel classification is identifying the number of objects
  //that appear in an image. Note that in this example, the number of output predictions is not
  //necessarily fixed, and could vary from image to image.
  //Classification Models in MLlib
  //Spark has several models available for performing binary and multiclass classification out of the
  //box. The following models are available for classification in Spark:
  //Logistic regression
  //Decision trees
  //Random forests
  //Gradient-boosted trees
  //Spark does not support making multilabel predictions natively. In order to train a multilabel
  //model, you must train one model per label and combine them manually

  // in Scala
  val bInput = spark.read.format("parquet").load("src/resources/binary-classification")
    .selectExpr("features", "cast(label as double) as label")

  bInput.show(false)
  bInput.describe().show(false)
  //so our dataset is really small here, in real life it should be at least a few hundred if not thousands of rows

  //https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression

  //Example
  //Here’s a simple example using the LogisticRegression model. Notice how we didn’t specify any
  //parameters because we’ll leverage the defaults and our data conforms to the proper column
  //naming. In practice, you probably won’t need to change many of the parameters:

  val lr = new LogisticRegression()
    .setMaxIter(150) //how many times the algorithm will run


  println(lr.explainParams()) // see all parameters
  //you can adjust parameters later as well
  lr.setMaxIter(50)
    .setElasticNetParam(0.7)
  val lrModel = lr.fit(bInput)

  //Once the model is trained you can get information about the model by taking a look at the
  //coefficients and the intercept. The coefficients correspond to the individual feature weights (each
  //feature weight is multiplied by each respective feature to compute the prediction) while the
  //intercept is the value of the italics-intercept (if we chose to fit one when specifying the model).
  //Seeing the coefficients can be helpful for inspecting the model that you built and comparing how
  //features affect the prediction:

  // in Scala
  println(lrModel.coefficients)
  println(lrModel.intercept)

  val myVector = Vectors.dense(1.0, 5.0, 9.6)

  println(lrModel.predict(myVector)) //will provide me an answer 0 or 1 in this case
  //then you decide what 0 represents and what 1 represents

  //Decision Trees
  //Decision trees are one of the more friendly and interpretable models for performing classification
  //because they’re similar to simple decision models that humans use quite often. For example, if
  //you have to predict whether or not someone will eat ice cream when offered, a good feature
  //might be whether or not that individual likes ice cream. In pseudocode, if
  //person.likes(“ice_cream”), they will eat ice cream; otherwise, they won’t eat ice cream. A
  //decision tree creates this type of structure with all the inputs and follows a set of branches when
  //it comes time to make a prediction. This makes it a great starting point model because it’s easy to
  //reason about, easy to inspect, and makes very few assumptions about the structure of the data. In
  //short, rather than trying to train coeffiecients in order to model a function, it simply creates a big
  //tree of decisions to follow at prediction time. This model also supports multiclass classification
  //and provides outputs as predictions and probabilities in two different columns.
  //While this model is usually a great start, it does come at a cost. It can overfit data extremely
  //quickly. By that we mean that, unrestrained, the decision tree will create a pathway from the start
  //based on every single training example. That means it encodes all of the information in the
  //training set in the model. This is bad because then the model won’t generalize to new data (you
  //will see poor test set prediction performance). However, there are a number of ways to try and
  //rein in the model by limiting its branching structure (e.g., limiting its height) to get good
  //predictive power


  val dt = new DecisionTreeClassifier()
    .setMaxDepth(5) //you do not want the tree to grow too large (that is ask too many questions..)
    //default is in fact 5 so no need for above setter

  val dtModel = dt.fit(bInput)

  //so decision tree predictor will work just like the logistic regression model
  println(dtModel.predict(myVector))

}
