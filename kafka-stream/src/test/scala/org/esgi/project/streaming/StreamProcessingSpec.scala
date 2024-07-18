package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.streaming.models.Trade
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
  test("Topology should compute a correct trade count per symbol") {
    // Given
    val trades = List(
      Trade("trade", 123456789, "BNBBTC", 12345, "0.001", "100", 88, 50, 123456785, true, true),
      Trade("trade", 123456790, "BNBBTC", 12346, "0.002", "150", 89, 51, 123456786, false, true),
      Trade("trade", 123456791, "ETHBTC", 12347, "0.003", "200", 90, 52, 123456787, true, true)
    )

    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )

    val tradeTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.tradeTopic,
        Serdes.stringSerde.serializer(),
        implicitly[Serde[Trade]].serializer()
      )

    val tradeCountStore: KeyValueStore[String, Long] =
      topologyTestDriver
        .getKeyValueStore[String, Long](
          StreamProcessing.tradeCountStoreName
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
