package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestInputTopic, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.streaming.models.Trade
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport with BeforeAndAfterEach {
  var topologyTestDriver: TopologyTestDriver = _
  var tradeTopic: TestInputTopic[String, Trade] = _
  var tradeCountStore: KeyValueStore[String, Long] = _

  override def beforeEach(): Unit = {
    // Initialize the TopologyTestDriver and other resources before each test
    topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
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
}
