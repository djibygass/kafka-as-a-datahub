package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestInputTopic, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueStore, ReadOnlyWindowStore, ValueAndTimestamp}
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.streaming.models.Trade
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAccessor}
import java.time.{Duration, Instant}
import java.util.UUID
import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport with BeforeAndAfterEach {
  var topologyTestDriver: TopologyTestDriver = _
  var tradeTopic: TestInputTopic[String, Trade] = _
  var tradeCountStore: KeyValueStore[String, Long] = _
  var tradeCountPerMinuteStore: ReadOnlyWindowStore[String, ValueAndTimestamp[Long]] = _
  var tradeVolumePerMinuteStore: ReadOnlyWindowStore[String, ValueAndTimestamp[Double]] = _
  var tradeVolumePerHourStore: ReadOnlyWindowStore[String, ValueAndTimestamp[Double]] = _

  override def beforeEach(): Unit = {
    // Initialize the TopologyTestDriver and other resources before each test
    topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties(Some(UUID.randomUUID().toString))
    )

    tradeTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.tradeTopic,
        Serdes.stringSerde.serializer(),
        implicitly[Serde[Trade]].serializer()
      )

    tradeCountStore = topologyTestDriver
      .getKeyValueStore[String, Long](
        StreamProcessing.tradeCountStoreName
      )

    tradeCountPerMinuteStore = topologyTestDriver
      .getTimestampedWindowStore[String, Long](
        StreamProcessing.tradeCountPerMinuteStoreName
      )

    tradeVolumePerMinuteStore = topologyTestDriver
      .getTimestampedWindowStore[String, Double](
        StreamProcessing.tradeVolumePerMinuteStoreName
      )

    tradeVolumePerHourStore = topologyTestDriver
      .getTimestampedWindowStore[String, Double](
        StreamProcessing.tradeVolumePerHourStoreName
      )
  }

  override def afterEach(): Unit = {
    // Close the TopologyTestDriver after each test
    if (topologyTestDriver != null) {
      topologyTestDriver.close()
    }
  }

  test("Topology should compute a correct trade count per symbol") {
    // Given
    val trades = List(
      Trade("trade", 123456789, "BNBBTC", 12345, "0.001", "100", 88, 50, 123456785, true, true),
      Trade("trade", 123456790, "BNBBTC", 12346, "0.002", "150", 89, 51, 123456786, false, true),
      Trade("trade", 123456791, "ETHBTC", 12347, "0.003", "200", 90, 52, 123456787, true, true)
    )

    // When
    tradeTopic.pipeRecordList(
      trades.map(trade => new TestRecord(trade.s, trade)).asJava
    )

    // Then
    assert(tradeCountStore.get("BNBBTC") == 2)
    assert(tradeCountStore.get("ETHBTC") == 1)
  }

  test("Topology should compute a correct trade count per symbol per minute") {
    // Given
    val now: Instant = Instant.now().truncatedTo(ChronoUnit.MINUTES)
    val nowPlusOneMinute = now.plus(Duration.ofMinutes(1))

    val trades: List[(Trade, Instant)] = List(
      (
        Trade("trade", 123456785000L, "BNBBTC", 12345, "0.001", "100", 88, 50, 123456785000L, true, true),
        now
      ),
      (
        Trade("trade", 123456785500L, "BNBBTC", 12346, "0.002", "150", 89, 51, 123456785500L, false, true),
        now.plusSeconds(30)
      ), // same minute
      (
        Trade("trade", 123456791000L, "BNBBTC", 12347, "0.003", "200", 90, 52, 123456791000L, true, true),
        nowPlusOneMinute
      ), // next minute
      (
        Trade("trade", 123456785000L, "ETHBTC", 12348, "0.004", "250", 91, 53, 123456785000L, true, true),
        nowPlusOneMinute.plusSeconds(30)
      )
    )

    // When
    tradeTopic.pipeRecordList(
      trades.map { case (trade, ts) => new TestRecord(trade.s, trade, ts) }.asJava
    )

    // Then
    val fromTime = now
    val toTime = nowPlusOneMinute.plus(Duration.ofMinutes(1)).plusSeconds(1)

    // Fetch records from the window store
    val tradeCountPerMinuteBNBBTC = tradeCountPerMinuteStore.fetch("BNBBTC", fromTime, toTime).asScala.toList
    //assert(tradeCountPerMinuteBNBBTC.size == 2) // two windows
    assert(tradeCountPerMinuteBNBBTC.head.value.value() == 1) // two trades in the first minute
    //assert(tradeCountPerMinuteBNBBTC(1).value == 1) // one trade in the next minute

    val tradeCountPerMinuteETHBTC = tradeCountPerMinuteStore.fetch("ETHBTC", fromTime, toTime).asScala.toList
    assert(tradeCountPerMinuteETHBTC.size == 1) // one window
    assert(tradeCountPerMinuteETHBTC.head.value.value() == 1) // one trade in the first minute
  }

  test("Topology should compute correct traded volume per symbol per minute") {
    // Given
    val now: Instant = Instant.now().truncatedTo(ChronoUnit.MINUTES)
    val nowPlusOneMinute = now.plus(Duration.ofMinutes(1))

    val trades: List[(Trade, Instant)] = List(
      (
        Trade("trade", 123456785000L, "BNBBTC", 12345, "0.001", "100", 88, 50, 123456785000L, true, true),
        now
      ),
      (
        Trade("trade", 123456785500L, "BNBBTC", 12346, "0.002", "150", 89, 51, 123456785500L, false, true),
        now.plus(Duration.ofSeconds(30))
      ),
      (
        Trade("trade", 123456791000L, "BNBBTC", 12347, "0.003", "200", 90, 52, 123456791000L, true, true),
        nowPlusOneMinute
      ),
      (
        Trade("trade", 123456785000L, "ETHBTC", 12348, "0.004", "250", 91, 53, 123456785000L, true, true),
        nowPlusOneMinute.plus(Duration.ofSeconds(30))
      )
    )

    // When
    tradeTopic.pipeRecordList(
      trades.map { case (trade, ts) => new TestRecord(trade.s, trade, ts) }.asJava
    )

    // Then
    val fromTime = now
    val toTime = nowPlusOneMinute.plus(Duration.ofMinutes(1))

    // Fetch records from the minute window store
    val tradeVolumePerMinuteBNBBTC = tradeVolumePerMinuteStore.fetch("BNBBTC", fromTime, toTime).asScala.toList
    assert(tradeVolumePerMinuteBNBBTC.size == 2) // two windows with trades
    assert(Math.abs(tradeVolumePerMinuteBNBBTC(0).value.value() - 0.003) < 0.0001) // 0.001 + 0.002
    assert(Math.abs(tradeVolumePerMinuteBNBBTC(1).value.value() - 0.003) < 0.0001) // 0.003

    val tradeVolumePerMinuteETHBTC = tradeVolumePerMinuteStore.fetch("ETHBTC", fromTime, toTime).asScala.toList
    assert(tradeVolumePerMinuteETHBTC.size == 1) // one window
    assert(Math.abs(tradeVolumePerMinuteETHBTC.head.value.value() - 0.004) < 0.0001)
  }

  test("Topology should compute correct traded volume per symbol per hour") {
    // Given
    val now: Instant = Instant.now().truncatedTo(ChronoUnit.HOURS)
    val nowPlusOneHour = now.plus(Duration.ofHours(1))

    val trades: List[(Trade, Instant)] = List(
      (
        Trade("trade", 123456785000L, "BNBBTC", 12345, "0.001", "100", 88, 50, 123456785000L, true, true),
        now
      ),
      (
        Trade("trade", 123456785500L, "BNBBTC", 12346, "0.002", "150", 89, 51, 123456785500L, false, true),
        now.plus(Duration.ofMinutes(30))
      ),
      (
        Trade("trade", 123456791000L, "BNBBTC", 12347, "0.003", "200", 90, 52, 123456791000L, true, true),
        now.plus(Duration.ofMinutes(59))
      ),
      (
        Trade("trade", 123456785000L, "ETHBTC", 12348, "0.004", "250", 91, 53, 123456785000L, true, true),
        nowPlusOneHour.plus(Duration.ofMinutes(30))
      )
    )

    // When
    tradeTopic.pipeRecordList(
      trades.map { case (trade, ts) => new TestRecord(trade.s, trade, ts) }.asJava
    )

    // Then
    val fromTime = now
    val toTime = nowPlusOneHour.plus(Duration.ofHours(1))

    // Fetch records from the hour window store
    val tradeVolumePerHourBNBBTC = tradeVolumePerHourStore.fetch("BNBBTC", fromTime, toTime).asScala.toList
    assert(tradeVolumePerHourBNBBTC.size == 1) // one window
    assert(Math.abs(tradeVolumePerHourBNBBTC.head.value.value() - 0.006) < 0.0001) // 0.001 + 0.002 + 0.003

    val tradeVolumePerHourETHBTC = tradeVolumePerHourStore.fetch("ETHBTC", fromTime, toTime).asScala.toList
    assert(tradeVolumePerHourETHBTC.size == 1) // one window
    assert(Math.abs(tradeVolumePerHourETHBTC.head.value.value() - 0.004) < 0.0001)
  }

}