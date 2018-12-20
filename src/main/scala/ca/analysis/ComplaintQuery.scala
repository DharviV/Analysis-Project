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


object ComplaintQuery extends Tokenizer {
    

    val spark= SparkSession
      .builder()
        .appName("COMPLAINTS ANALYSIS")
          .config("spark.some.config.option", "complaints")
            .getOrCreate()

              import spark.implicits._
                
                def main(args: Array[String]) {
                     
                      val df = spark.read.csv("nypdData")
                      val F="F"
                      val BRONX="BRONX"

 df.filter($"_c34".contains(F)).groupBy("_c8","_c34").count().show

          //   df.where("_c13=BRONX").count()            

             df.filter($"_c13".contains(BRONX)).groupBy("_c8", "_C34").count().show()
                    //  df.groupBy("_c13").count().show()   
               //     df.filter("year(_c1)=2017").groupBy("_c13").count().show() 

                   
              //      df.select(date_format(col("_c1"),"DD")).show()
                  //   df.groupBy("_c13", "_c12", "_yearVal").count().show()

        //           df.withColumn("_c3",date_format(to_date(col("_c3"),"MM/DD/YYYY"),"MM/DD/YYYY")).show()


//        df.withColumn("_c1",from_unixtime(unix_timestamp(col("_c1"),"MM/DD/YYYY"),"MM/DD/YYYY")).show()

// df.select($"_c1",date_format(to_date($"_c1","MM/DD/YYYY"),"YYYY").alias("modified")).show()

                   //.groupBy("_c13", "_c12").count().show()
                     //     df.printSchema()
                              
                              
                            }
}
