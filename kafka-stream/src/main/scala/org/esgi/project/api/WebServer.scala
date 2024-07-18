package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import java.util.Properties
import scala.collection.JavaConverters._

object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("visits" / Segment) { period: String =>
        get {
          complete(
            List(VisitCountResponse("", 0))
          )
        }
      },
      path("latency" / "beginning") {
        get {
          complete(
            List(MeanLatencyForURLResponse("", 0))
          )
        }
      },
      path("trades") {
        get {
          complete {
            val consumerProps = new Properties()
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "web-server-group")
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

            val consumer = new KafkaConsumer[String, String](consumerProps)
            consumer.subscribe(java.util.Collections.singletonList("trades"))

            try {
              val records = consumer.poll(java.time.Duration.ofSeconds(10)).asScala.toList
              val jsonData = records.map { record =>
                Json.obj(
                  "key" -> record.key(),
                  "value" -> record.value()
                )
              }

              Json.toJson(jsonData)
            } finally {
              consumer.close()
            }
          }
        }
      }
    )
  }
}
