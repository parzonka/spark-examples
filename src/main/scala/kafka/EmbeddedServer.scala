package kafka

import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector.embedded.KafkaProducer
import com.datastax.spark.connector.embedded.EmbeddedKafka
import org.apache.spark.streaming.api.java.JavaPairInputDStream

/**
 * Start this first, then either the producer or consumer
 */
object EmbeddedServer {

  def main(args: Array[String]): Unit = {

    /** Starts the Kafka broker. */
    lazy val kafka = new EmbeddedKafka()

    val topic = "streaming.test.topic"
    val group = "streaming.test.group"

    kafka.createTopic(topic)

    while (true) {
      Thread.sleep(10000)
    }

  }

}
