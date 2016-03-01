package kafka

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
 * Returns incoming messages in the @Link{EmbeddedServer}
 */
object EmbeddedKafkaStreaming {

  def main(args: Array[String]): Unit = {

    val topic = "streaming.test.topic"


    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
      "auto.offset.reset" -> "smallest",
      "zookeeper.connect" -> "2181",
      "auto.commit.enable" -> "true",
      "auto.commit.interval.ms" -> "1000")

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaTest").set("spark.app.id", "KafkaTest")
    val ssc = new StreamingContext(conf, Seconds(1))

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))

    stream.print()

    var offsetRanges = Array[OffsetRange]()
    stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to termination
  }

}
