package streaming

import org.scalatest.FunSuite
import org.apache.spark._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD

class NetWorkWordCountSuite extends FunSuite {

  /**
   * Test transformation logic.
   */
  test("Words should be counted correctly") {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[2]"))

    val rdd = sc.parallelize(List("foo", "bar", "foo"))

    val verify = assertResult(Array(("foo", 2), ("bar", 1))) _
    verify(NetworkWordCount.countWords(rdd).collect());

  }

}
