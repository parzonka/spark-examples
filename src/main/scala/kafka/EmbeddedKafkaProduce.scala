package kafka

import org.apache.kafka.clients.producer._
import collection.JavaConversions._
import java.util.Properties

/**
 * Sends a message to the @Link{EmbeddedServer}
 */
object EmbeddedKafkaProduce {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "127.0.0.1:9092")
    props.put("acks", "all")
    props.put("retries", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topic = "streaming.test.topic"
    val group = "streaming.test.group"

    val producer = new KafkaProducer[String, String](props)
    
    // send a timestamped message
    producer.send(new ProducerRecord[String, String](topic, group, "msg: " + System.currentTimeMillis())).get()

  }

}
