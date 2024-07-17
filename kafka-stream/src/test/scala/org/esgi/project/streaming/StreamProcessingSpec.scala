//package org.esgi.project.streaming
//
//import io.github.azhur.kafka.serde.PlayJsonSupport
//import org.apache.kafka.streams.TopologyTestDriver
//import org.apache.kafka.streams.scala.serialization.Serdes
//import org.apache.kafka.streams.state.{KeyValueStore, WindowStore}
//import org.apache.kafka.streams.test.TestRecord
//import org.scalatest.BeforeAndAfterEach
//import org.scalatest.funsuite.AnyFunSuite
//import play.api.libs.json.Json
//
//import java.time.Instant
//import scala.jdk.CollectionConverters._
//
//class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport with BeforeAndAfterEach {
//
//  var topologyTestDriver: TopologyTestDriver = _
//
//  override def beforeEach(): Unit = {
//    topologyTestDriver = new TopologyTestDriver(
//      StreamProcessing.builder.build(),
//      StreamProcessing.buildProperties
//    )
//  }
//
//  override def afterEach(): Unit = {
//    if (topologyTestDriver != null) {
//      topologyTestDriver.close()
//    }
//  }
//
//  test("Topology should compute the number of trades per pair per minute") {
//    // Given
//    val trades = List(
//      Json.stringify(Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10000", "q" -> "0.1", "T" -> 123456789L)),
//      Json.stringify(Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "BTCUSD", "p" -> "10020", "q" -> "0.2", "T" -> 123456789L)),
//      Json.stringify(Json.obj("e" -> "trade", "E" -> 123456789L, "s" -> "ETHUSD", "p" -> "200", "q" -> "1", "T" -> 123456789L))
//    )
//
//    val tradeTopic = topologyTestDriver.createInputTopic(
//      StreamProcessing.tradeTopic,
//      Serdes.stringSerde.serializer(),
//      Serdes.stringSerde.serializer(),
//      Instant.ofEpochMilli(0L), // Start timestamp
//      java.time.Duration.ofMinutes(1) // Advance each record by 1 minute
//    )
//
//    val tradeCountsStore: WindowStore[String, Long] = topologyTestDriver
//      .getWindowStore[String, Long]("trade-counts-store")
//
//    // When
//    tradeTopic.pipeRecordList(trades.map(trade => new TestRecord[String, String](null, trade)).asJava)
//
//    // Vérifiez le contenu du magasin d'état après l'envoi des messages
//    val tradeCountsIterator = tradeCountsStore.fetch("BTCUSD", Instant.ofEpochMilli(0L), Instant.ofEpochMilli(60000L))
//    val tradeCounts = tradeCountsIterator.asScala.toList
//
//    println(s"Trade counts for BTCUSD: ${tradeCounts.map(_.value).mkString(", ")}")
//
//    // Then
//    assert(tradeCounts.map(_.value).sum == 2, "Expected 2 trades for BTCUSD")
//  }
//
//  // Tests similaires pour les autres statistiques...
//}

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

  // Tests similaires pour les autres statistiques...
}
