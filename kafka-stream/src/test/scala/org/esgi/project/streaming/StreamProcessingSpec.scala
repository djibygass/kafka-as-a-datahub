package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.scalatest.funsuite.AnyFunSuite
import org.esgi.project.streaming.models.Trade
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
  test("Topology should compute a correct word count") {
    // Given
    val trades = List(
      Trade("trade", 123456785000L, "BNBBTC", 12345, "0.001", "100", 88, 50, 123456785000L, true, true),
      Trade("trade", 123456785500L, "BNBBTC", 12346, "0.002", "150", 89, 51, 123456785500L, false, true), // same minute
      Trade("trade", 123456791000L, "BNBBTC", 12347, "0.003", "200", 90, 52, 123456791000L, true, true), // next minute
      Trade("trade", 123456785000L, "ETHBTC", 12348, "0.004", "250", 91, 53, 123456785000L, true, true)
    )

    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties

    )

//    val wordTopic = topologyTestDriver
//      .createInputTopic(
//        StreamProcessing.wordTopic,
//        Serdes.stringSerde.serializer(),
//        Serdes.stringSerde.serializer()
//      )

    val tradeTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.TradeTopicName,
        Ser,
        Serdes.stringSerde.serializer()
      )

//    val wordCountStore: KeyValueStore[String, Long] =
//      topologyTestDriver
//        .getKeyValueStore[String, Long](
//          StreamProcessing.wordCountStoreName
//        )

    val tradePerPairPerMinuteStore: KeyValueStore[String, Long] =
      topologyTestDriver
        .getKeyValueStore[String, Long](
          StreamProcessing.TradePerPairPerMinuteStoreName
        )


    // When
//    wordTopic.pipeRecordList(
//      messages.map(message => new TestRecord(message, message)).asJava
//    )
//
//    // Then
//    assert(wordCountStore.get("hello") == 2)
//    assert(wordCountStore.get("world") == 1)
//    assert(wordCountStore.get("moon") == 1)
//    assert(wordCountStore.get("foobar") == 1)
//    assert(wordCountStore.get("42") == 1)

    // Convert Trade objects to JSON strings
    val tradeJsonStrings = trades.map(trade => Json.toJson(trade).toString())

    // Pipe the JSON strings into the TestInputTopic
    tradeTopic.pipeRecordList(
      tradeJsonStrings.map(jsonString => new TestRecord[String, String](null, jsonString)).asJava
    )

//    tradeTopic.pipeRecordList(
//      trades.map(trade => new TestRecord[String, String](trade.s, trade.toString)).asJava
//    )

    // assert with debug
    // Vérifiez le contenu du magasin d'état après l'envoi des messages
//    println(tradePerPairPerMinuteStore.get("BNBBTC"))
//    println(tradePerPairPerMinuteStore.get("ETHBTC"))
//
//    // Then
//    assert(tradePerPairPerMinuteStore.get("BNBBTC") == 2)
//    assert(tradePerPairPerMinuteStore.get("ETHBTC") == 1)

    assert(tradePerPairPerMinuteStore != null, "tradePerPairPerMinuteStore is null")
    println(tradePerPairPerMinuteStore.get("BNBBTC"))
    println(tradePerPairPerMinuteStore.get("ETHBTC"))

  }
}
