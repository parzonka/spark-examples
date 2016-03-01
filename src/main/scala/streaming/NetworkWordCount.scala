package streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

/**
 * Produce input using shell with nc -lk 9998
 */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9998
    val socketTextStream = ssc.socketTextStream("localhost", 9998)

    // configure transformation
    def wordsWithCountsAsTuples = (rddOfWords: RDD[(String)]) => countWords(rddOfWords).collect()

    // process distributed task
    socketTextStream.foreachRDD(rdd => {
      wordsWithCountsAsTuples(rdd).foreach(println)
      println("===== " + System.currentTimeMillis)
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to termination
  }

  /**
   * Maps words into tuples: (word,count)
   */
  def countWords(lines: RDD[String]): RDD[(String, Int)] = {
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts: RDD[(String, Int)] = pairs.reduceByKey(_ + _)
    wordCounts
  }
}
