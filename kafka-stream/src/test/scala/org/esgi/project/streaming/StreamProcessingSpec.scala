package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueStore, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json.Json

import java.time.Instant
import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport with BeforeAndAfterEach {

  var topologyTestDriver: TopologyTestDriver = _

  override def beforeEach(): Unit = {
    topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )
  }

  override def afterEach(): Unit = {
    if (topologyTestDriver != null) {
      topologyTestDriver.close()
    }
  }

  test("Topology should compute the number of trades per pair per minute") {
    // Given
    val trades = List(
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10000", "q" -> "0.1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(0L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10020", "q" -> "0.2", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(1L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "ETHUSD", "p" -> "200", "q" -> "1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(2L)
      )
    )

    val tradeTopic = topologyTestDriver.createInputTopic(
      StreamProcessing.tradeTopic,
      Serdes.stringSerde.serializer(),
      Serdes.stringSerde.serializer()
    )

    val tradeCountsStore: WindowStore[String, Long] = topologyTestDriver
      .getWindowStore("trade-counts-store")

    // When
    tradeTopic.pipeRecordList(trades.asJava)

    // Vérifiez le contenu du magasin d'état après l'envoi des messages
    val tradeCountsIterator = tradeCountsStore.fetch("BTCUSD", Instant.ofEpochMilli(0L), Instant.ofEpochMilli(60000L))
    val tradeCounts = tradeCountsIterator.asScala.toList

    println(s"Trade counts for BTCUSD: ${tradeCounts.map(_.value).mkString(", ")}")

    // Then
    assert(tradeCounts.map(_.value).sum == 2, "Expected 2 trades for BTCUSD")
  }

  test("Topology should compute the average price per pair per minute") {
    // Given
    val trades = List(
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10000", "q" -> "0.1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(0L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10020", "q" -> "0.2", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(1L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "ETHUSD", "p" -> "200", "q" -> "1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(2L)
      )
    )

    val tradeTopic = topologyTestDriver.createInputTopic(
      StreamProcessing.tradeTopic,
      Serdes.stringSerde.serializer(),
      Serdes.stringSerde.serializer()
    )

    val totalPricesAndCountsStore: WindowStore[String, (Double, Long)] = topologyTestDriver
      .getWindowStore("total-prices-and-counts-store")

    // When
    tradeTopic.pipeRecordList(trades.asJava)

    // Vérifiez le contenu du magasin d'état après l'envoi des messages
    val totalPricesAndCountsIterator =
      totalPricesAndCountsStore.fetch("BTCUSD", Instant.ofEpochMilli(0L), Instant.ofEpochMilli(60000L))
    val totalPricesAndCounts = totalPricesAndCountsIterator.asScala.toList

    println(s"Total prices and counts for BTCUSD: ${totalPricesAndCounts.map(_.value).mkString(", ")}")

    // Then
    val totalPrices = totalPricesAndCounts.map(_.value._1)
    val totalCounts = totalPricesAndCounts.map(_.value._2)
    val averagePrice = if (totalCounts.sum > 0) totalPrices.sum / totalCounts.sum else 0.0
    assert(averagePrice == 10010.0, s"Expected average price of 10010 but got $averagePrice")
  }

  test("Topology should compute OHLC prices and volume per pair per minute") {
    // Given
    val trades = List(
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10000", "q" -> "0.1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(0L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10020", "q" -> "0.2", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(1L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "ETHUSD", "p" -> "200", "q" -> "1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(2L)
      )
    )

    val tradeTopic = topologyTestDriver.createInputTopic(
      StreamProcessing.tradeTopic,
      Serdes.stringSerde.serializer(),
      Serdes.stringSerde.serializer()
    )

    val ohlcStore: WindowStore[String, (Double, Double, Double, Double, Double)] = topologyTestDriver
      .getWindowStore("ohlc-and-volume-store")

    // When
    tradeTopic.pipeRecordList(trades.asJava)

    // Vérifiez le contenu du magasin d'état après l'envoi des messages
    val ohlcIterator = ohlcStore.fetch("BTCUSD", Instant.ofEpochMilli(0L), Instant.ofEpochMilli(60000L))
    val ohlc = ohlcIterator.asScala.toList

    println(s"OHLC for BTCUSD: ${ohlc.map(_.value).mkString(", ")}")

    // Then
    val ohlcValues = ohlc.map(_.value)
    assert(ohlcValues.nonEmpty, "Expected non-empty OHLC values for BTCUSD")
    val (open, close, low, high, volume) = ohlcValues.head
    assert(open == 10000, s"Expected open price of 10000 but got $open")
    assert(close == 10020, s"Expected close price of 10020 but got $close")
    assert(low == 10000, s"Expected low price of 10000 but got $low")
    assert(high == 10020, s"Expected high price of 10020 but got $high")
    assert(math.abs(volume - 0.3) < 1e-6, s"Expected volume of 0.3 but got $volume")
  }

  test("Topology should compute volume per pair per hour") {
    // Given
    val trades = List(
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10000", "q" -> "0.1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(0L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10020", "q" -> "0.2", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(1L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "ETHUSD", "p" -> "200", "q" -> "1", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(2L)
      ),
      new TestRecord[String, String](
        "key",
        Json.stringify(
          Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10000", "q" -> "0.3", "T" -> 123456789L)
        ),
        Instant.ofEpochMilli(3600000L) // After one hour
      )
    )

    val tradeTopic = topologyTestDriver.createInputTopic(
      StreamProcessing.tradeTopic,
      Serdes.stringSerde.serializer(),
      Serdes.stringSerde.serializer()
    )

    val hourlyVolumeStore: WindowStore[String, Double] = topologyTestDriver
      .getWindowStore("hourly-volume-store")

    // When
    tradeTopic.pipeRecordList(trades.asJava)

    // Vérifiez le contenu du magasin d'état après l'envoi des messages
    val volumeIterator1 = hourlyVolumeStore.fetch("BTCUSD", Instant.ofEpochMilli(0L), Instant.ofEpochMilli(3600000L))
    val volumeIterator2 =
      hourlyVolumeStore.fetch("BTCUSD", Instant.ofEpochMilli(3600000L), Instant.ofEpochMilli(7200000L))
    val volumes1 = volumeIterator1.asScala.toList
    val volumes2 = volumeIterator2.asScala.toList

    println(
      s"Hourly volume for BTCUSD (window 0-1h): ${volumes1.map(kv => s"[${kv.key}, ${kv.value}]").mkString(", ")}"
    )
    println(
      s"Hourly volume for BTCUSD (window 1-2h): ${volumes2.map(kv => s"[${kv.key}, ${kv.value}]").mkString(", ")}"
    )

    // Then
    val totalVolume = (volumes1 ++ volumes2).map(_.value).sum
    assert(math.abs(totalVolume - 0.6) < 1e-6, s"Expected total volume of 0.6 but got $totalVolume")
  }
}
