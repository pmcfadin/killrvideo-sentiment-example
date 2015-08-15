package com.datastax.example.killrvideo

import org.apache.spark.{Logging, SparkContext, SparkConf}
import com.datastax.spark.connector._


case class positiveWordSet(word: String)

object Sentiment {
  def main(args: Array[String]): Unit = {



    // the setMaster("local") lets us run & test the job right in our IDE
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    // "local" here is the master, meaning we don't explicitly have a spark master set up
    val sc = new SparkContext("local", "killrvideo", conf)

    val words = sc.cassandraTable("killrvideo", "sentiment_words").select("word","word_type")

    val positiveWords = words.filter(row => (row.getString("word_type") == "positive")).collect().map(row => row.getString("word")).toSet
    val negativeWords = words.filter(row => (row.getString("word_type") == "negative")).collect().map(row => row.getString("word")).toSet

    // keyspace & table
    val rdd = sc.cassandraTable("killrvideo", "comments_by_video").select("comment")

    // get a simple count of all the rows in the table
    val result = rdd.count()

    if (positiveWords.contains("humor"))
      println("Found humor")

    //
    val positiveWordCount = rdd.map(row => row.getString("comment").split("\\s+")).collect().take(5)

    //println positiveWordCount

      //.filter(word => (positiveWords.contains(word)))
      //.reduceByKey(_ + _).top(5).foreach(println)

    println("Sentiment word count", words.count())
    println("Positive word count", positiveWords.size)
    println("Negative word count", negativeWords.size)

    println("RDD Count", result)
  }
}