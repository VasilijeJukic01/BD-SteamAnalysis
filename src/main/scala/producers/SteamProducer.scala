package producers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import constants.Constants.{API_URL, KAFKA_SERVER, KAFKA_TOPIC}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import java.util.Properties

object SteamProducer {

  @volatile var running = true

  private def createProducer(): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", KAFKA_SERVER)
    properties.put("key.serializer", classOf[StringSerializer].getName)
    properties.put("value.serializer", classOf[StringSerializer].getName)

    new KafkaProducer[String, String](properties)
  }

  private def startProducer(): Unit = {
    val producer = createProducer()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // Console Thread
    val consoleThread = new Thread(() => {
      while (running) {
        val input = scala.io.StdIn.readLine()
        if (input.trim == "close") {
          println("Stream ended by user.")
          running = false
          producer.send(new ProducerRecord[String, String](KAFKA_TOPIC, "close"))
          producer.close()
          System.exit(0)
        }
      }
    })
    consoleThread.setDaemon(true)
    consoleThread.start()

    // API
    while (running) {
      val source = Source.fromURL(API_URL)
      source.getLines().foreach { line =>
        if (!running) return
        if (line.trim.nonEmpty && !line.trim.startsWith("event:") && !line.trim.startsWith("id:")) {
          val jsonMessage = try {
            val cleanedLine = if (line.startsWith("data:")) line.substring(5).trim else line.trim
            mapper.writeValueAsString(Map("data" -> cleanedLine))
          } catch {
            case e: Exception =>
              println(s"Failed to serialize message: $line, error: ${e.getMessage}")
              ""
          }

          if (jsonMessage.nonEmpty) {
            println(s"Sending message: $jsonMessage")
            val record = new ProducerRecord[String, String](KAFKA_TOPIC, jsonMessage)
            producer.send(record)
          }
        }
      }
      source.close()
    }
  }

  def main(args: Array[String]): Unit = {
    startProducer()
  }
}