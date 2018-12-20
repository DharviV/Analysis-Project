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




object OffenseLocation extends Tokenizer {
    

    val spark= SparkSession
      .builder()
        .appName("Category of Offense")
          .config("spark.some.config.option", "configval")
            .getOrCreate()

              import spark.implicits._
                
                def main(args: Array[String]) {
                     
                      val df = spark.read.csv("nypdData")
                         
                          val BRONX="BRONX"
                              val MANHATTAN="MANHATTAN"


      //to get date
    df.createOrReplaceTempView("table1")

     val bdf = spark.sql("""select from_unixtime(unix_timestamp(_c1, 'MM/dd/yyyy')) as new_format, _c1, _c2, _c8, _c13, _c15, _c0 from table1""")
      // bdf.printSchema
      

     val bbdf = bdf.withColumn("dt",$"new_format".cast("date"))
        bbdf.printSchema

    bbdf.filter((year($"dt")).contains(2017) && ($"_c13").contains(MANHATTAN)).groupBy($"_c13", $"_c8", $"_c15").count().sort(desc("count")).show
   
    bbdf.filter((year($"dt")).contains(2017) && ($"_c13").contains(MANHATTAN)).groupBy($"_c13", $"_c8").count().sort(desc("count")).show
    
                }
}

