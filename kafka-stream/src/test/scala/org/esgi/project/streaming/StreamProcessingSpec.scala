package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json.Json

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

  test("Topology should compute the number of trades per pair") {
    // Given
    val trades = List(
      Json.stringify(
        Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10000", "q" -> "0.1", "T" -> 123456789L)
      ),
      Json.stringify(
        Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10020", "q" -> "0.2", "T" -> 123456789L)
      ),
      Json.stringify(
        Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "ETHUSD", "p" -> "200", "q" -> "1", "T" -> 123456789L)
      )
    )

    val tradeTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.tradeTopic,
        Serdes.stringSerde.serializer(),
        Serdes.stringSerde.serializer()
      )

    val statsStore: KeyValueStore[String, Long] =
      topologyTestDriver
        .getKeyValueStore[String, Long](
          StreamProcessing.statsStoreName
        )

    // When
    tradeTopic.pipeRecordList(
      trades.map(trade => new TestRecord[String, String](null, trade)).asJava
    )

    // Vérifiez le contenu du magasin d'état après l'envoi des messages
    println(s"BTCUSD count: ${statsStore.get("BTCUSD")}")
    println(s"ETHUSD count: ${statsStore.get("ETHUSD")}")

    // Then
    assert(statsStore.get("BTCUSD") == 2, "Expected 2 trades for BTCUSD")
    assert(statsStore.get("ETHUSD") == 1, "Expected 1 trade for ETHUSD")
  }
}
