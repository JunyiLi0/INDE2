import java.util.Properties
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.DefaultFormats
import java.awt.{SystemTray, TrayIcon}
import java.awt.TrayIcon.MessageType


object KafkaConsumerExample {
  def main(args: Array[String]): Unit = {

    val inputTopic = "harmony-topic"
    val outputTopic = "preprocessed-topic"
    val bootstrapServers = "localhost:9092" // Replace with your Kafka bootstrap servers

    // Configure Kafka consumer properties
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", bootstrapServers)
    consumerProps.put("key.deserializer", classOf[StringDeserializer].getName)
    consumerProps.put("value.deserializer", classOf[StringDeserializer].getName)
    consumerProps.put("group.id", "consumer-group")
    val consumer = new KafkaConsumer[String, String](consumerProps)

    // Configure Kafka producer properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", bootstrapServers)
    producerProps.put("key.serializer", classOf[StringSerializer].getName)
    producerProps.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](producerProps)

    try {
      consumer.subscribe(java.util.Collections.singletonList(inputTopic))
      consumeRecords(consumer, producer)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
      producer.close()
    }
  }

  def consumeRecords(consumer: KafkaConsumer[String, String], producer: KafkaProducer[String, String]): Unit = {
    val records = consumer.poll(java.time.Duration.ofMillis(100))
    val iterator = records.iterator()

    if (iterator.hasNext) {
      processRecord(iterator, producer)
    }
    else {
        Thread.sleep(1000)
    }
    consumeRecords(consumer, producer)
  }

  def processRecord(iterator: java.util.Iterator[org.apache.kafka.clients.consumer.ConsumerRecord[String,String]], producer: KafkaProducer[String, String]): Unit = {
    if (iterator.hasNext) {
        val record = iterator.next()
        implicit val formats: Formats = DefaultFormats
        val jsonString = record.value()

        val json = parse(jsonString)
        val timestamp = (json \ "timestamp").extract[String]
        val x = (json \ "longitude").extract[String]
        val y = (json \ "latitude").extract[String]
        val harmonyscores = (json \ "harmonyscores").extract[Map[String, String]]

        val filteredHarmonyscores = harmonyscores.filter{ case (_, harmonyScore) => harmonyScore.toInt < 20}

        val filteredJsonString = compact(render(filteredHarmonyscores))

        val preprocessedJson = ("timestamp" -> timestamp) ~ ("longitude" -> x) ~ ("latitude" -> y) ~ ("filteredHarmonyscores" -> filteredJsonString)
        val preprocessedJsonString = compact(render(preprocessedJson))
        val outputRecord = new ProducerRecord[String, String]("preprocessed-topic", preprocessedJsonString)

        if (filteredHarmonyscores.nonEmpty) {
          producer.send(outputRecord)
          println("Preprocessed message: " + preprocessedJsonString)
          val alertMessage = s"An element in the output stream requires attention!\nTimestamp: $timestamp\nLongitude: $x\nLatitude: $y"
          sendAlert(alertMessage)
        }
        processRecord(iterator, producer)
    }
  }

  def sendAlert(message: String): Unit = {
    if (SystemTray.isSupported()) {
      val tray = SystemTray.getSystemTray()
      val trayIcon = new TrayIcon(java.awt.Toolkit.getDefaultToolkit().getImage("../../../ressources/alert.png"))

      trayIcon.setImageAutoSize(true)
      trayIcon.setToolTip("Alert")
      tray.add(trayIcon)

      trayIcon.displayMessage("Alert", message, MessageType.INFO)
    } else {
      println("System tray is not supported on this platform.")
    }
  }
}
