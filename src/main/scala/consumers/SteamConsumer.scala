package consumers

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import constants.Constants.{KAFKA_SERVER, KAFKA_TOPIC, OUTPUT_DIR}
import schemas.SteamSchema.schema
import mapper.SteamDataMapper.mapFields

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

object SteamConsumer {

  private def createConsumer(): KafkaConsumer[String, String] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", KAFKA_SERVER)
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[StringDeserializer].getName)
    properties.put("group.id", "steam-group")
    properties.put("auto.offset.reset", "earliest")

    new KafkaConsumer[String, String](properties)
  }

  // Core
  private def startConsumer(): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaConsumerToParquet")
      .master("local[*]")
      .getOrCreate()

    val consumer = createConsumer()
    consumer.subscribe(Collections.singletonList(KAFKA_TOPIC))

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    var running = true

    while (running) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))

      val data = records.asScala.flatMap { record =>
        val message = record.value()
        try {
          val json = mapper.readValue(message, classOf[Map[String, Any]])

          if (json.contains("data") && json("data").toString == "close") {
            println("Stream ended by producer.")
            running = false
            None
          }
          else if (json.contains("data")) {
            val rawData = try {
              mapper.readValue(json("data").toString, classOf[Map[String, Any]])
            } catch {
              case e: Exception =>
                println(s"Failed to parse JSON from data field: ${json("data")}, error: ${e.getMessage}")
                Map.empty[String, Any]
            }

            Some(mapFields(rawData))
          }
          else None
        } catch {
          case e: Exception =>
            println(s"Skipping invalid message: $message, error: ${e.getMessage}")
            None
        }
      }

      if (data.nonEmpty) {
        val rows = data.map { row =>
          Row.fromSeq(schema.fields.map(field => row.getOrElse(field.name, null)))
        }
        val df = spark.createDataFrame(rows.toSeq.asJava, schema)

        if (!df.isEmpty) {
          println("Writing non-empty DataFrame to Parquet.")
          df.write.mode(SaveMode.Append).parquet(OUTPUT_DIR)
        }
        else {
          println("DataFrame is empty, skipping write.")
        }
      }
      else {
        println("No valid data received in this batch.")
      }
    }

    consumer.close()
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    startConsumer()
  }
}