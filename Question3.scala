package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Question3 { // Create a SparkSession
  def main(args: Array[String]): Unit = {
    //Remove info messages, show only ERRORS
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Create SparkSession
    val spark = SparkSession.builder()
      .appName("Movie Analysis")
      .master("local[*]")
      .getOrCreate()

    //Read the csv file into the DataFrame
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv("/home/giannispierr/Desktop/Σχολη 2022/Πλη47/Ergasia4/a4q3Test/src/input/movies.csv")

    //Q1 - Movies per genre, sort by alphabetical order
    val moviesPerGenreDF = moviesDF
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre")
      .agg(
        countDistinct("movieId").alias("movies")
      )
      .orderBy("genre")

    //Print Q1 Test
    println("Question 1: How many movies per genre")
    moviesPerGenreDF.show()

    //Q2 - Movies per year (top 10) - sort by
    val moviesPerYearDF = moviesDF
      .withColumn("year", regexp_extract(col("title"), "\\((\\d{4})\\)", 1))
      .groupBy("year")
      .agg(
        countDistinct("movieId").alias("movies")
      )
      .orderBy(col("movies").desc)
      .limit(10)

    //Print Q2 Test
    println("Question 2: How many movies per year")
    moviesPerYearDF.show()

      //Q3 - Most common word among movies titles
      val wordCountDF = moviesDF
        .withColumn("words", split(lower(regexp_replace(col("title"), "[^a-zA-Z\\s]", "")), "\\s+"))
        .select(explode(col("words")).alias("word"))
        .filter(length(col("word")) >= 4 && regexp_replace(col("word"), "\\D", "") === "")
        .groupBy(col("word"))
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc)

    //Q3 Test print
    println("Question 3: Most common words")
    wordCountDF.show(10, false)
  }
}


