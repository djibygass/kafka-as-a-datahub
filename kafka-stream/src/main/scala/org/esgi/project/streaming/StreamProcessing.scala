package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.esgi.project.streaming.models.Trade
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream._

import java.util.Properties
import java.time.Duration
import java.util.UUID

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"binance-stream-app"

  private val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val TradeTopicName = "trade"

  val TradePerPairPerMinuteStoreName = "TradePerPairPerMinute"

  implicit val tradeSerde: Serde[Trade] = toSerde[Trade]

  // topic sources
  val trades = builder.stream[String, Trade](TradeTopicName)

  val tradesGroupedByPair: KGroupedStream[String, Trade] = trades.groupBy((_, trade) => trade.s)

  // This table should be materialized using the TradePerPairPerMinuteStoreName variable as store name

  val tradesPerPairPerMinute: KTable[Windowed[String], Long] = tradesGroupedByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .count()(Materialized.as(TradePerPairPerMinuteStoreName))

//  val wordCounts: KTable[String, Long] = words
//    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
//    .groupBy((_, word) => word)
//    .count()(Materialized.as(wordCountStoreName))

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
  def buildProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}
