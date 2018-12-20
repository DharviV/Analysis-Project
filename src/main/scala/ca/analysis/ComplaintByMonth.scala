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

               object ComplaintByMonth extends Tokenizer {
                   val log = Logger.getLogger(getClass().getName())


                     def main(args: Array[String]) {
                          
                            val conf = new SparkConf().setAppName("Analysis 1")
                                  val sc = new SparkContext(conf)


                                        val data= sc.textFile("nypdData")

                                              val splitLines=data.map(x=> (x.split(",")(0),x.split(",")(1)))


                                                     
                                                     val filterDate=data.flatMap(x=>{
                                                                
                                                                
                                                                val crimeNum=x.split(",")(0)
                                                                         
                                                                         val date=x.split(",")(1)
                                                                                  
                                                                                  
                                                                                  
                                                                                  val splitDate=date.split("/")(0)
                                                                                           
                                                                                        
                                                                                           Some(splitDate,1)
                                                                                                    
                                                                                                  }).reduceByKey(_ + _)
                                                              
                                                             
                                                              
                                                        
                                                              
                                                             val byMonth=filterDate.top(14).foreach(line=>println(line))
                                                                      
                                                                      
                                                                      
                                                                  
                                                                       }

               }

