package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.esgi.project.streaming.models._

import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"trade-statistics-app"

  private val props: Properties = buildProperties()

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val tradeTopic = "trades"
  val tradeCountStoreName = "trade-count-store"
  val tradeCountPerMinuteStoreName = "trade-count-per-minute-store"
  val tradeVolumePerMinuteStoreName = "trade-volume-per-minute-store"
  val tradeVolumePerHourStoreName = "trade-volume-per-hour-store"

  val trades: KStream[String, Trade] = builder.stream[String, Trade](tradeTopic)

  // Count trades per symbol
  val tradeCounts: KTable[String, Long] = trades
    .groupBy((_, trade) => trade.s)
    .count()(Materialized.as(tradeCountStoreName))

  // Count trades per symbol per minute
  val tradeCountsPerMinute: KTable[Windowed[String], Long] = trades
    .groupBy((_, trade) => trade.s)
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(tradeCountPerMinuteStoreName))

  // Calculate traded volume per symbol per minute
  val tradeVolumePerMinute: KTable[Windowed[String], Double] = trades
    .groupBy((_, trade) => trade.s)
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .aggregate(0.0)((_, trade, total) => total + trade.q.toDouble)(Materialized.as(tradeVolumePerMinuteStoreName))

  // Calculate traded volume per symbol per hour
  val tradeVolumePerHour: KTable[Windowed[String], Double] = trades
    .groupBy((_, trade) => trade.s)
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
    .aggregate(0.0)((_, trade, total) => total + trade.q.toDouble)(Materialized.as(tradeVolumePerHourStoreName))

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties(appName: Option[String] = None): Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName.getOrElse(applicationName))
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}
