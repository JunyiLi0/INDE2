import java.util.Properties
import org.apache.kafka.clients.producer._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    val topic = "harmony-topic"
    val bootstrapServers = "localhost:9092" // Replace with your Kafka bootstrap servers
    val numberOfDrones = 10
    val numberMaxOfCitizen = 10

    // Configure Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")


      val droneIterator = Iterator.continually {
        val drones = (0 until numberOfDrones).toList.map { drone =>
          val longitude = getRandomCoordinate(-180, 180)
          val latitude = getRandomCoordinate(-90, 90)

          val citizens = (0 until scala.util.Random.nextInt(numberMaxOfCitizen)).map(_ => {
          val citizenId = scala.util.Random.nextInt(100)
          val harmonyScore = scala.util.Random.nextInt(101)
          (citizenId, harmonyScore)
        })

          val harmonyScores = citizens.map { case (citizenId, harmonyScore) =>
            s""""$citizenId": "$harmonyScore""""
          }.mkString("{ ", ", ", " }")

          val words = citizens.map { case (_, harmonyScore) =>
            getWordsBasedOnHarmonyScore(harmonyScore)
          }.mkString(", ")
          val timestamp = LocalDateTime.now().format(formatter)

          val jsonString = s"""{"id":$drone, "timestamp":"$timestamp", "longitude":$longitude, "latitude":$latitude, "harmonyscores":$harmonyScores, "words":"$words" },"""

          val record = new ProducerRecord[String, String](topic, jsonString)
          producer.send(record)

          // Sleep for 20000 milliseconds (20 seconds)
          println("Sent message: " + jsonString)
  
          
          jsonString
        }
        Thread.sleep(20000)
        drones
      }.flatten

      val filteredDroneIterator = droneIterator.takeWhile(_ => true).foreach(_ => ())

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
