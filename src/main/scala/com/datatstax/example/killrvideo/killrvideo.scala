package com.datatstax.example

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.kafka._

/**
 * Created by patrick on 6/14/15.
 */
object killrvideo {

  def main(args: Array[String]): Unit = {


    // the setMaster("local") lets us run & test the job right in our IDE
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    // "local" here is the master, meaning we don't explicitly have a spark master set up
    val sc = new SparkContext("local", "killrvideo", conf)

    val commentsByVideordd = sc.cassandraTable("killrvideo", "comments_by_video").select("comment")

    val solrQueryRDD = sc.cassandraTable("killrvideo", "videos").select("name").where("solr_query='tags:crime*'")

    solrQueryRDD.collect().map(row => println(row.getString("name")))

    val cc = new CassandraSQLContext(sc)

    cc.setKeyspace("killrvideo")

    val sqlRDD = cc.cassandraSql("SELECT cast(videoid as String) videoid, count(*) c FROM comments_by_video GROUP BY cast(videoid as String) ORDER BY c DESC limit 10")

    sqlRDD.collect().foreach(println)



    //val count = solrQueryRDD.count()

    //println("Count =", count)

  }
}
