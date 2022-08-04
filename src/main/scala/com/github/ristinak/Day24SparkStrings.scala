package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.Encoders.INT
import org.apache.spark.sql.functions.{col, desc, exp, expr, initcap, length, lit, lower, lpad, ltrim, regexp_extract, regexp_replace, rpad, rtrim, size, translate, trim, upper}

object Day24SparkStrings extends App {
  println("CH6: Working with Strings")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  //Working with Strings
  //String manipulation shows up in nearly every data flow, and it’s worth explaining what you can
  //do with strings. You might be manipulating log files performing regular expression extraction or
  //substitution, or checking for simple string existence, or making all strings uppercase or
  //lowercase.
  //Let’s begin with the last task because it’s the most straightforward. The initcap function will
  //capitalize every word in a given string when that word is separated from another by a space

  df.select(col("Description"),
    initcap(col("Description"))).show(3, false)

  //all SQL functions are listed here; https://spark.apache.org/docs/latest/api/sql/index.html
  spark.sql("SELECT Description, initcap(Description) FROM dfTable")
    .show(3, false) //false shows full strings in columns, without truncation/cutting

  //As just mentioned, you can cast strings in uppercase and lowercase, as well:

  df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))) //the lower here is not necessary of course example shows nesting
    .show(3, false)

  spark.sql("SELECT Description, lower(Description), " +
    "upper(lower(Description)) FROM dfTable") //again upper(lower is not needed but it might be useful for some other function
    .show(3, false)

  //Another trivial task is adding or removing spaces around a string. You can do this by using lpad,
  //ltrim, rpad and rtrim, trim:

  df.select(
    col("CustomerId"), //not needed just to show you we are working with the dateframe
    ltrim(lit(" HELLO ")).as("ltrim"),
    rtrim(lit(" HELLO ")).as("rtrim"),
    trim(lit(" HELLO ")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp"),
    rpad(lit("HELLO"), 10, " ").as("rp"),
    //😁 is represented by 4 bytes thats why you have \u twice
    lpad(rpad(lit("HELLO"), 10, "*"), 15, "\uD83D\uDE01").as("pad15charstotal")
  ).show(2)
  //so pad even works with high value unicode after 128k which is smileys

  //so lpad (rpad is similar)
  //Left-pad the string column with pad to a length of len.
  // If the string column is longer than len, the return value is shortened to len characters.
  println("SQL padding:")
  spark.sql(
    """
      |SELECT
      | CustomerId,
      |ltrim(' HELLLOOOO ') as ltrim,
      |rtrim(' HELLLOOOO '),
      |trim(' HELLLOOOO '),
      |lpad('HELLOOOO ', 3, ' '),
      |rpad('HELLOOOO ', 10, ' ')
      | FROM dfTable
      |""".stripMargin)
    .show(2)


  //Regular Expressions
  //Probably one of the most frequently performed tasks is searching for the existence of one string
  //in another or replacing all mentions of a string with another value. This is often done with a tool
  //called regular expressions that exists in many programming languages. Regular expressions give
  //the user an ability to specify a set of rules to use to either extract values from a string or replace
  //them with some other values.
  //Spark takes advantage of the complete power of Java regular expressions. The Java regular
  //expression syntax departs slightly from other programming languages, so it is worth reviewing
  //before putting anything into production. There are two key functions in Spark that you’ll need in
  //order to perform regular expression tasks: regexp_extract and regexp_replace. These
  //functions extract values and replace values, respectively.
  //Let’s explore how to use the regexp_replace function to replace substitute color names in our
  //description column:

  //prepping regex in Scala - could do it by hand of course by writing up full regex
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  println(regexString) //"BLACK|WHITE|RED|GREEN|BLUE"

  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description"))
    .show(5, false)

  spark.sql(
    """
      |SELECT
      |regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'colorful') as
      |color_clean, Description
      |FROM dfTable
      |""".stripMargin)
    .show(5, false)

  //TODO open up March 1st, of 2011 CSV
  //Select Capitalized Description Column
  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  //ideally there would be even number of _______LATVIA__________ (30 total)
  //select Description column again with all occurences of metal or wood replaced with material
  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns

  val filePath2011March1 = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df2011March1 = readDataWithView(spark, filePath2011March1)

  //prepping regex for materials
  val simpleMaterials = Seq("metal", "wood", "felt")
  val regexStringMaterials = simpleMaterials.map(_.toUpperCase).mkString("|")

  df2011March1.select(
    initcap(lower(col("Description"))),
//    length returns column, int needed:
//    lpad(rpad(col("Country"), length(col("Country"))/2 + 15, "_"), 30, "_"),
//    lpad(rpad(col("Country"), col("Country").toString().length/2 + 15, "_"), 30, "_"),
//    lpad(rpad(col("Country"), expr("Country").toString().length/2 + 15, "_"), 30, "_"),
    expr("lpad(rpad(Country, 15+int((CHAR_LENGTH(Country))/2), '_'), 30, '_') as ___Country___"),
    regexp_replace(col("Description"), regexStringMaterials, "material").as("Unknown material"))
    .orderBy(col("Description")) // to get different countries and materials
    .show(10, false)

  spark.sql(
    """
      |SELECT Description,
      |Country,
      |rpad(Country, 22, '_'),
      |lpad(Country, 22, '_'),
      |lpad(rpad(Country, 15+int((CHAR_LENGTH(Country))/2), '_'), 30, '_') as ___Country___
      |FROM dfTable
      |""".stripMargin)
    .sample(false, fraction = 0.3)
    .show(50,false) //thank goodness we only have 1300 rows and our sample is 30 percent of that...

  //Another task might be to replace given characters with other characters. Building this as a
  //regular expression could be tedious, so Spark also provides the translate function to replace these
  //values. This is done at the character level and will replace all instances of a character with the
  //indexed character in the replacement string

  //so we give a sort of dictionary acttually a string of values which ar to be replaced by matching character
  //in another string

  //no need for LEET to 1337 because LET to 137 does the same thing
  // L -> 1
  // E -> 3
  // I -> 1 so no problem with translating 2 characters to 1 the same
  // T -> 7

  //so basically a translation for single characters to other characters
  // resulting characters could be same
  df.select(translate(col("Description"), "LEIT", "1317"), col("Description"))
    .show(5, false)

  //so the idea is to extract some value out of particular row and basically have it in a new column
  val simpleColors1 = Seq("black", "white", "red", "green", "blue")
  val regexStringForExtraction = simpleColors1.map(_.toUpperCase).mkString("(", "|", ")") //notice parenthesis
  //regex101.com

  df.select(
    regexp_extract(col("Description"), regexStringForExtraction, 1).alias("color_clean"),
    col("Description"))
    .where("CHAR_LENGTH(color_clean)>0")
    .show(10)

  //Sometimes, rather than extracting values, we simply want to check for their existence. We can do
  //this with the contains method on each column. This will return a Boolean declaring whether the
  //value you specify is in the column’s string

  //so we add a new column(with boolean whether there is black or white in description)
  //then filter by that column

  // in Scala
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(5, false)

  //SQL with instr function
  spark.sql(
    """
      |SELECT Description FROM dfTable
      |WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
      |""".stripMargin)
    .show(5, false)

  //This is trivial with just two values, but it becomes more complicated when there are values.
  //Let’s work through this in a more rigorous way and take advantage of Spark’s ability to accept a
  //dynamic number of arguments. When we convert a list of values into a set of arguments and pass
  //them into a function, we use a language feature called varargs. Using this feature, we can
  //effectively unravel an array of arbitrary length and pass it as arguments to a function. This,
  //coupled with select makes it possible for us to create arbitrary numbers of columns
  //dynamically:

  val multipleColors = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = multipleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value //we need this to select the rest of the columns

  df.select(selectedColumns:_*). //we unroll our sequence of Columsn into multiple individual arguments
    //because select takes multiple columns one by one NOT an Sequence of columns
    show(10, false)

  //so I do not have to give all selected columns
  df.select(selectedColumns.head, selectedColumns(3), selectedColumns.last, col("Description"))
    .show(5, false)

  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)

  //  val numbers = Seq(1,5,6,20,5)
  //will not work on println since it does not strictly speaking support *-parameters
  //  println("Something", numbers:_*) //unrolls a sequence(Array here) will print a tuple of numbers since it is equivalent TO
  //  println(1,5,6,20,5)

  //so check if your method or function has * at the end of some parameter then you can unroll some sequence into those parameters


}

