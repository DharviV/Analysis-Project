package ca.analysis

import io.bespin.scala.util.Tokenizer
import org.apache.spark.sql.SparkSession

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.broadcast
import org.rogach.scallop._
import scala.collection.mutable
import org.apache.spark.sql.types.{DateType, IntegerType}

import org.apache.spark.sql.functions._

object ComplaintDates extends Tokenizer {

  val spark= SparkSession
    .builder()
      .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
          .getOrCreate()

            import spark.implicits._
              
              def main(args: Array[String]) {
                   
                    val df = spark.read.csv("nypdData")


     // for each borough and category of crime, how many crimes are reported after the day of incidence of the crime. That is the exact day of
     //occurence of the reported event (or starting day of occurence of the event) is different from the date the event was reported to police
     //         
     //             
            df.filter($"_c1"  =!= "_c6").groupBy("_c12","_c13").count().show()
                 

  // if the two dates are the exact same 
 df.filter(col("_c1").contains(col("_c6"))).groupBy("_c12","_c13").count().show()

// different dates
df.filter(!(col("_c1").contains(col("_c6")))).groupBy("_c12","_c13").count().show()


              }

}
