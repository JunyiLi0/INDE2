import java.util.Properties
import org.apache.kafka.clients.producer._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    val topic = "harmony-topic"
    val bootstrapServers = "localhost:9092" // Replace with your Kafka bootstrap servers
    val numberOfDrones = 9

    // Configure Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

      while (true) {
        val drones = (0 until numberOfDrones).toList

        drones.foreach { drone =>
          val latitude = getRandomCoordinate(-90, 90)
          val longitude = getRandomCoordinate(-180, 180)
          val harmonyScore = scala.util.Random.nextInt(101)
          val words = getWordsBasedOnHarmonyScore(harmonyScore)
          val timestamp = LocalDateTime.now().format(formatter)

          val jsonString =
            s"""{"id":$drone, "timestamp":"$timestamp", "x":$latitude, "y":$longitude, "harmonyscore":$harmonyScore, "words":"$words"}"""

          val record = new ProducerRecord[String, String](topic, jsonString)
          producer.send(record)

          println("Sent message: " + jsonString)
        }

        // Sleep for 60000 for 1 minute
        Thread.sleep(20000)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
  }

  def getRandomCoordinate(min: Int, max: Int): Double = {
    min + scala.util.Random.nextDouble() * (max - min)
  }

  def getWordsBasedOnHarmonyScore(harmonyScore: Int): String = {
    harmonyScore match {
      case s if s <= 20 => "I am a bad guy"
      case s if s > 20 && s <= 80 => "I am a normal guy"
      case _ => "I am a good guy"
    }
  }
}
