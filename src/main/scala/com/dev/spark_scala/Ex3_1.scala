package com.dev.spark_scala


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

//
// in this exercise you will experiment with various Spark operations
// you are not required to make alterations to this code!
//

object Ex3_1 {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Spark Operations").setMaster("local")
        val sparkContext = new SparkContext(conf)
        val sqlContext = new SQLContext(sparkContext)

        args(0) match {
          
            case "text" => {
              
                // load a text file and demonstrate that it is read line by line
                //
                val textRDD = sparkContext.textFile("/home/kmoh19/spark/crs1262/data/legislators/sou-2012")
                textRDD.foreach( line => {
                    println( "***" )
                    println( line )
                    println( "***" )
                    println
                })
            }
          
            case "sentence" => {

                // instruct the underlying Hadoop TextInputFormat to split the text data on the "." character
                // NOTE: sentences don't align with line ending
                //
                sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", ".")
                val textRDD = sparkContext.textFile("/home/kmoh19/spark/crs1262/data/legislators/sou-2012")
                textRDD.foreach( sentence => {
                    println( "***" )
                    println( sentence )
                    println( "***" )
                    println
                })
            }
            
            case "jdbc" => {
              
                // load the rows of a table in an external database
                // this example connects to MYSQL using JDBC
                //
                val options: Map[String, String] = Map(
                    "url" -> "jdbc:mysql://localhost/movies?user=spark&password=apache",
                    "driver" -> "com.mysql.jdbc.Driver",
                    "dbtable" -> "ratings")

                // load the "ratings" table from the locally installed MySQL, returns a DataFrame
                // NOTE 1: could dump the 1st 20 row using the show() method, this also displays the schema
                //         used println() so as not to hide behind any magic...
                //      2: there are various way to pulls rows apart, could use simple column indexes
                //         i.e. row(0), row(1), ..., etc.
                //
                val jdbcDF = sqlContext.read.format("jdbc").options(options).load()
                val rows = jdbcDF.take(20)
                rows.foreach( row => {
                    println( "***" )
                    println( s"${row(0)}, ${row(1)}, ${row(2)}, ${row(3)}")
                    println( "***" )
                    println
                })
            }
            
            case "lazy" => {

                // demonstrate the lazy evaluation of Spark transforms
                // apply some transforms to a "loaded" file, but the file will be missing!
                // the load won't happen so no processing/errors will occur
                //
                // NOTE: at this stage the "lazy-data" directory should not exist!
                //
                val textRDD = sparkContext.textFile("/home/kmoh19/spark/crs1262/exercises/Exercise-3.1/lazy-data")
                val wordCount = textRDD.flatMap( _.split(" ") )
                                       .map( _.filter(Character.isLetter(_)) )
                                       .map( (_, 1) )
                                       
                println("*** Finished, were any exceptions thrown?")
                println("*** The data wasn't loaded as none of the above operations were triggered by an action")
            }
            
            case "lazy-action" => {

                // as above, but this time with operations that trigger the load and processing
                // i.e. reduceByKey() and collect()
                //
                // NOTE 1: for the first run don't create the "lazy-data" directory, an exception will be thrown
                //      2: for the second run, create the director and copy in text-file.txt
                //
                try {
                    val textRDD = sparkContext.textFile("/home/kmoh19/spark/crs1262/exercises/Exercise-3.1/lazy-data")
                    val wordCount = textRDD.flatMap( _.split(" ") )
                                           .map( _.filter(Character.isLetter(_)) )
                                           .map( (_, 1) )
                                           .reduceByKey( _ + _ )
                    val counts = wordCount.collect()
                    counts.foreach( println )
                }
                catch {
                    // the directory is missing (first run, you've not created it yet...)
                    //
                    case e: Exception => println(s"*** Unable to load files in the lazy-data directory\n$e")
                }
            }
            
            case "save" => {
              
                // load a dataset and display the number of partition
                // save the dataset to demonstrate that each partition is saved in a separate file
                //
                // NOTE: you will need to delete the "results" directory between runs
                //
                val textRDD = sparkContext.textFile("/home/kmoh19/spark/crs1262/data/legislators/sou-2012")
                println(s"*** The text file has been loaded into ${textRDD.getNumPartitions} partitions")
                
                textRDD.saveAsTextFile("/home/spark/crs1262/exercises/Exercise-3.1/results")
            }
            
            case "partition" => {
              
              // same as above but re-distribute the data to 5 partitions
              // e.g. to increase performance in CPU bound applications through additional parallelism
              //
              // NOTE: you will need to delete the "repartitioned-results" directory between runs
              //
              val textRDD = sparkContext.textFile("/home/kmoh19/spark/crs1262/data/legislators/sou-2012")
              val textData = textRDD.repartition(5)
              println(s"*** The text file has been loaded into ${textData.getNumPartitions} partitions")

              textData.saveAsTextFile("/home/spark/crs1262/exercises/Exercise-3.1/repartitioned-results")
            }
            
            case _ => {
              
                // missing command line option, provide help!
                //
                println("*** Please provide a command line argument")
                println("*** Options: text, sentence, jdbc, lazy, lazy-action, save, partition")
            }
        }
 
        sparkContext.stop()
    }
}