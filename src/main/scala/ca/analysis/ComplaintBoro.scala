package ca.analysis

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.broadcast
import org.rogach.scallop._
import scala.collection.mutable
import org.apache.spark.sql.SparkSession

import java.util.regex.Pattern

object ComplaintBoro extends Tokenizer {
    val log = Logger.getLogger(getClass().getName())


      def main(args: Array[String]) {
           
             val conf = new SparkConf().setAppName("Analysis 1")
                   val sc = new SparkContext(conf)


                         val data= sc.textFile("nypdData")

                         val line=data.toString.split(",")
                            
                               val filtering=data.flatMap(x=>{
                                       
                            val el12=x.split(",")(12).toString()     
                            val el13=x.split(",")(13).toString()
                                  
                                  val el14=x.split(",")(14).toString()
                                        
                                        val el15=x.split(",")(15).toString()

                                        val el16=x.split(",")(16).toString()
                                              
                                              val crime=x.split(",")(0)
                                 
                                                               
                            val BROOKLYN="BROOKLYN"
                            val STATEN="STATEN ISLAND"
                            val MANHATTAN="MANHATTAN"
                            val BRONX="BRONX"
                            val QUEENS="QUEENS"

              if(QUEENS.equalsIgnoreCase(el13) || QUEENS.equalsIgnoreCase(el14) || QUEENS.equalsIgnoreCase(el15) || QUEENS.equalsIgnoreCase(el16) || 
                     QUEENS.equalsIgnoreCase(el12) )
                                                                               
                                                                              Some(QUEENS,1)
                                                                                  
                                                                                 else
                                                                                          None
                                                                                             
                                                                                             
                                                                                              })

                               val adding=filtering.reduceByKey(_+_)

                                     
                                  val printboro=adding.take(5).foreach(println)
                          
                          }
}




