package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
  test("Topology should compute a correct word count") {
    // Given
    val messages = List(
      "hello world",
      "hello moon",
      "foobar",
      "42"
    )

    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )

    val wordTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.wordTopic,
        Serdes.stringSerde.serializer(),
        Serdes.stringSerde.serializer()
      )

    val wordCountStore: KeyValueStore[String, Long] =
      topologyTestDriver
        .getKeyValueStore[String, Long](
          StreamProcessing.wordCountStoreName
        )

    // When
    wordTopic.pipeRecordList(
      messages.map(message => new TestRecord(message, message)).asJava
    )

    // Then
    assert(wordCountStore.get("hello") == 2)
    assert(wordCountStore.get("world") == 1)
    assert(wordCountStore.get("moon") == 1)
    assert(wordCountStore.get("foobar") == 1)
    assert(wordCountStore.get("42") == 1)
  }
}
