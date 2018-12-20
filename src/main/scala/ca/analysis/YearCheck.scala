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

object YearCheck extends Tokenizer {
    

    val spark= SparkSession
      .builder()
        .appName("Spark SQL basic example")
          .config("spark.some.config.option", "some-value")
            .getOrCreate()

              import spark.implicits._
                
                def main(args: Array[String]) {
                     
                      val df = spark.read.csv("nypdData")
                         
                          val BRONX="BRONX"

                         df.createOrReplaceTempView("table1")
                             val bdf = spark.sql("""select from_unixtime(unix_timestamp(_c1, 'MM/dd/yyyy')) as new_format, _c1, _c2 from table1""")
                                  
                              bdf.printSchema
                                     
                                    val bbdf = bdf.withColumn("dt",$"new_format".cast("date"))
                                        
                                        bbdf.printSchema


                                         //  to get date and year - important
                                         //     //bbdf.select(year($"dt"), $"_c1").show
                      

                          bbdf.filter((year($"dt")).contains(2017)).select($"_c1").show


                     //    df.withColumn("year", year(to_date(df("_c1"), "MM/DD/YYYY").cast("date"))).show()


                          //    df.groupBy("_c13").count().show()
                            
                                 
                               //   df.withColumn("year", year(to_date(df("_c1"), "MM/DD/YYYY"))).show()
                                      

                                      
                                    }
}
