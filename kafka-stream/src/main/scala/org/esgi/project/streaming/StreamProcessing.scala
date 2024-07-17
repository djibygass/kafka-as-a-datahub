//package org.esgi.project.streaming
//
//import io.github.azhur.kafka.serde.PlayJsonSupport
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.producer.ProducerConfig
//import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
//import org.apache.kafka.streams.scala._
//import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
//import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
//import org.apache.kafka.streams.state.Stores
//import play.api.libs.json._
//
//import java.time.Duration
//import java.util.Properties
//
//object StreamProcessing extends PlayJsonSupport {
//
//  import org.apache.kafka.streams.scala.ImplicitConversions._
//  import org.apache.kafka.streams.scala.serialization.Serdes._
//
//  val applicationName = s"trade-statistics-app"
//
//  private val props: Properties = buildProperties
//
//  // defining processing graph
//  val builder: StreamsBuilder = new StreamsBuilder
//
//  val tradeTopic = "trades"
//  val statsStoreName = "trade-stats-store"
//
//  case class TradeEvent(e: String, E: Long, s: String, p: String, q: String, T: Long)
//
//  implicit val tradeEventFormat: OFormat[TradeEvent] = Json.format[TradeEvent]
//
//  val trades: KStream[String, String] = builder.stream[String, String](tradeTopic)
//
//  val tradeEvents: KStream[String, TradeEvent] = trades
//    .mapValues(value => Json.parse(value).as[TradeEvent])
//
//  // Compute number of trades per pair per minute
//  val tradeCounts: KTable[Windowed[String], Long] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .count()(
//      Materialized
//        .as("trade-counts-store")
//    )
//
//  // Compute average price per pair per minute
//  val totalPricesAndCounts: KTable[Windowed[String], (Double, Long)] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .aggregate[(Double, Long)]((0.0, 0L))((key: String, trade: TradeEvent, aggregate: (Double, Long)) =>
//      (aggregate._1 + trade.p.toDouble, aggregate._2 + 1)
//    )(
//      Materialized
//        .as("total-prices-and-counts-store")
//    )
//
//  val averagePrices: KTable[Windowed[String], Double] = totalPricesAndCounts
//    .mapValues { case (totalPrice, count) => totalPrice / count }
//
//  // Compute open, close, low, high prices and volume per pair per minute
//  val ohlcAndVolume: KTable[Windowed[String], (Double, Double, Double, Double, Double)] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .aggregate[(Double, Double, Double, Double, Double)](
//      (Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue, 0.0),
//      (key: String, trade: TradeEvent, aggregate: (Double, Double, Double, Double, Double)) => {
//        val openPrice = if (aggregate._1 == Double.MaxValue) trade.p.toDouble else aggregate._1
//        val closePrice = trade.p.toDouble
//        val lowPrice = math.min(aggregate._3, trade.p.toDouble)
//        val highPrice = math.max(aggregate._4, trade.p.toDouble)
//        val volume = aggregate._5 + trade.q.toDouble
//        (openPrice, closePrice, lowPrice, highPrice, volume)
//      }
//    )(
//      Materialized
//        .as[String, (Double, Double, Double, Double, Double)]("ohlc-and-volume-store")
//        .withKeySerde(JavaSerdes.String)
//        .withValueSerde(serdeFrom[(Double, Double, Double, Double, Double)])
//    )
//
//  def run(): KafkaStreams = {
//    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
//    streams.start()
//
//    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
//      override def run(): Unit = {
//        streams.close()
//      }
//    }))
//    streams
//  }
//
//  // auto loader from properties file in project
//  def buildProperties: Properties = {
//    val properties = new Properties()
//    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
//    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
//    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
//    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
//    properties
//  }
//}
//

//package org.esgi.project.streaming
//
//import io.github.azhur.kafka.serde.PlayJsonSupport
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.producer.ProducerConfig
//import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
//import org.apache.kafka.streams.scala._
//import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
//import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
//import org.apache.kafka.streams.state.Stores
//import play.api.libs.json._
//
//import java.time.Duration
//import java.util.Properties
//
//object StreamProcessing extends PlayJsonSupport {
//
//  import org.apache.kafka.streams.scala.ImplicitConversions._
//  import org.apache.kafka.streams.scala.serialization.Serdes._
//
//  val applicationName = s"trade-statistics-app"
//
//  private val props: Properties = buildProperties
//
//  // defining processing graph
//  val builder: StreamsBuilder = new StreamsBuilder
//
//  val tradeTopic = "trades"
//  val statsStoreName = "trade-stats-store"
//
//  case class TradeEvent(e: String, E: Long, s: String, p: String, q: String, T: Long)
//
//  implicit val tradeEventFormat: OFormat[TradeEvent] = Json.format[TradeEvent]
//
//  val trades: KStream[String, String] = builder.stream[String, String](tradeTopic)
//
//  val tradeEvents: KStream[String, TradeEvent] = trades
//    .mapValues(value => Json.parse(value).as[TradeEvent])
//
//  // Compute number of trades per pair per minute
//  val tradeCounts: KTable[Windowed[String], Long] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .count()(Materialized.as("trade-counts-store"))
//
//  // Compute average price per pair per minute
//  val totalPricesAndCounts: KTable[Windowed[String], (Double, Long)] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .aggregate[(Double, Long)](
//      initializer = (0.0, 0L)
//    )(
//      aggregator = (key: String, trade: TradeEvent, aggregate: (Double, Long)) =>
//        (aggregate._1 + trade.p.toDouble, aggregate._2 + 1)
//    )(Materialized.as("total-prices-and-counts-store"))
//
//  val averagePrices: KTable[Windowed[String], Double] = totalPricesAndCounts
//    .mapValues { case (totalPrice, count) => totalPrice / count }
//
//  // Compute open, close, low, high prices and volume per pair per minute
//  val ohlcAndVolume: KTable[Windowed[String], (Double, Double, Double, Double, Double)] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .aggregate[(Double, Double, Double, Double, Double)](
//      initializer = (Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue, 0.0)
//    )(
//      aggregator = (key: String, trade: TradeEvent, aggregate: (Double, Double, Double, Double, Double)) => {
//        val openPrice = if (aggregate._1 == Double.MaxValue) trade.p.toDouble else aggregate._1
//        val closePrice = trade.p.toDouble
//        val lowPrice = math.min(aggregate._3, trade.p.toDouble)
//        val highPrice = math.max(aggregate._4, trade.p.toDouble)
//        val volume = aggregate._5 + trade.q.toDouble
//        (openPrice, closePrice, lowPrice, highPrice, volume)
//      }
//    )(Materialized.as[Windowed[String], (Double, Double, Double, Double, Double)]("ohlc-and-volume-store"))
//
//  def run(): KafkaStreams = {
//    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
//    streams.start()
//
//    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
//      override def run(): Unit = {
//        streams.close()
//      }
//    }))
//    streams
//  }
//
//  // auto loader from properties file in project
//  def buildProperties: Properties = {
//    val properties = new Properties()
//    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
//    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
//    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
//    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
//    properties
//  }
//}

//
//package org.esgi.project.streaming
//
//import io.github.azhur.kafka.serde.PlayJsonSupport
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.producer.ProducerConfig
//import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
//import org.apache.kafka.streams.scala._
//import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
//import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
//import org.apache.kafka.streams.state.Stores
//import play.api.libs.json._
//
//import java.time.Duration
//import java.util.Properties
//
//object StreamProcessing extends PlayJsonSupport {
//
//  import org.apache.kafka.streams.scala.ImplicitConversions._
//  import org.apache.kafka.streams.scala.serialization.Serdes._
//
//  val applicationName = s"trade-statistics-app"
//
//  private val props: Properties = buildProperties
//
//  // defining processing graph
//  val builder: StreamsBuilder = new StreamsBuilder
//
//  val tradeTopic = "trades"
//  val statsStoreName = "trade-stats-store"
//
//  case class TradeEvent(e: String, E: Long, s: String, p: String, q: String, T: Long)
//
//  implicit val tradeEventFormat: OFormat[TradeEvent] = Json.format[TradeEvent]
//
//  val trades: KStream[String, String] = builder.stream[String, String](tradeTopic)
//
//  val tradeEvents: KStream[String, TradeEvent] = trades
//    .mapValues(value => Json.parse(value).as[TradeEvent])
//
//  // Compute number of trades per pair per minute
//  val tradeCounts: KTable[Windowed[String], Long] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .count()(Materialized.as("trade-counts-store"))
//
//  // Compute average price per pair per minute
//  val totalPricesAndCounts: KTable[Windowed[String], (Double, Long)] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .aggregate[(Double, Long)](
//      initializer = (0.0, 0L)
//    )(
//      aggregator = (key: String, trade: TradeEvent, aggregate: (Double, Long)) =>
//        (aggregate._1 + trade.p.toDouble, aggregate._2 + 1)
//    )(Materialized.as("total-prices-and-counts-store"))
//
//  val averagePrices: KTable[Windowed[String], Double] = totalPricesAndCounts
//    .mapValues { case (totalPrice, count) => totalPrice / count }
//
//  // Compute open, close, low, high prices and volume per pair per minute
//  val ohlcAndVolume: KTable[Windowed[String], (Double, Double, Double, Double, Double)] = tradeEvents
//    .groupBy((_, trade) => trade.s)
//    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//    .aggregate[(Double, Double, Double, Double, Double)](
//      initializer = (Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue, 0.0)
//    )(
//      aggregator = (key: String, trade: TradeEvent, aggregate: (Double, Double, Double, Double, Double)) => {
//        val openPrice = if (aggregate._1 == Double.MaxValue) trade.p.toDouble else aggregate._1
//        val closePrice = trade.p.toDouble
//        val lowPrice = math.min(aggregate._3, trade.p.toDouble)
//        val highPrice = math.max(aggregate._4, trade.p.toDouble)
//        val volume = aggregate._5 + trade.q.toDouble
//        (openPrice, closePrice, lowPrice, highPrice, volume)
//      }
//    )(
//      Materialized.as(
//        Stores.persistentWindowStore(
//          "ohlc-and-volume-store",
//          Duration.ofMinutes(1),
//          Duration.ofMinutes(1),
//          false
//        )
//      )
//    )
//
//  def run(): KafkaStreams = {
//    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
//    streams.start()
//
//    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
//      override def run(): Unit = {
//        streams.close()
//      }
//    }))
//    streams
//  }
//
//  // auto loader from properties file in project
//  def buildProperties: Properties = {
//    val properties = new Properties()
//    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
//    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
//    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
//    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
//    properties
//  }
//}

package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.state.Stores
import play.api.libs.json._

import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"trade-statistics-app"

  private val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val tradeTopic = "trades"
  val statsStoreName = "trade-stats-store"

  case class TradeEvent(e: String, E: Long, s: String, p: String, q: String, T: Long)

  implicit val tradeEventFormat: OFormat[TradeEvent] = Json.format[TradeEvent]

  val trades: KStream[String, String] = builder.stream[String, String](tradeTopic)

  val tradeEvents: KStream[String, TradeEvent] = trades
    .mapValues(value => Json.parse(value).as[TradeEvent])

  // Compute number of trades per pair per minute
  val tradeCounts: KTable[Windowed[String], Long] = tradeEvents
    .groupBy((_, trade) => trade.s)
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .count()(
      Materialized.as(
        Stores.persistentWindowStore(
          "trade-counts-store",
          Duration.ofMinutes(1),
          Duration.ofMinutes(1),
          false
        )
      )
    )

  // Compute average price per pair per minute
  val totalPricesAndCounts: KTable[Windowed[String], (Double, Long)] = tradeEvents
    .groupBy((_, trade) => trade.s)
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .aggregate[(Double, Long)](
      initializer = (0.0, 0L)
    )(
      aggregator = (key: String, trade: TradeEvent, aggregate: (Double, Long)) =>
        (aggregate._1 + trade.p.toDouble, aggregate._2 + 1)
    )(
      Materialized.as(
        Stores.persistentWindowStore(
          "total-prices-and-counts-store",
          Duration.ofMinutes(1),
          Duration.ofMinutes(1),
          false
        )
      )
    )

  val averagePrices: KTable[Windowed[String], Double] = totalPricesAndCounts
    .mapValues { case (totalPrice, count) => totalPrice / count }

  // Compute open, close, low, high prices and volume per pair per minute
  val ohlcAndVolume: KTable[Windowed[String], (Double, Double, Double, Double, Double)] = tradeEvents
    .groupBy((_, trade) => trade.s)
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .aggregate[(Double, Double, Double, Double, Double)](
      initializer = (Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue, 0.0)
    )(
      aggregator = (key: String, trade: TradeEvent, aggregate: (Double, Double, Double, Double, Double)) => {
        val openPrice = if (aggregate._1 == Double.MaxValue) trade.p.toDouble else aggregate._1
        val closePrice = trade.p.toDouble
        val lowPrice = math.min(aggregate._3, trade.p.toDouble)
        val highPrice = math.max(aggregate._4, trade.p.toDouble)
        val volume = aggregate._5 + trade.q.toDouble
        (openPrice, closePrice, lowPrice, highPrice, volume)
      }
    )(
      Materialized.as(
        Stores.persistentWindowStore(
          "ohlc-and-volume-store",
          Duration.ofMinutes(1),
          Duration.ofMinutes(1),
          false
        )
      )
    )

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
