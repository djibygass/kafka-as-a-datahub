package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.esgi.project.streaming.StreamProcessing
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import java.time.Instant
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
      },
      path("trades" / Segment / "stats") { pair: String =>
        get {
          complete {
            val now: Instant = Instant.now()
            val oneHourAgo = now.minusSeconds(3600)

            // Store pour le volume des trades
            val tradeVolumeStore: ReadOnlyWindowStore[String, Double] =
              streams.store(
                StoreQueryParameters.fromNameAndType(
                  StreamProcessing.tradeVolumePerHourStoreName,
                  QueryableStoreTypes.windowStore[String, Double]()
                )
              )

            // Store pour le prix moyen
            val averagePriceStore: ReadOnlyWindowStore[String, (Double, Long)] =
              streams.store(
                StoreQueryParameters.fromNameAndType(
                  StreamProcessing.averagePricePerMinuteStoreName,
                  QueryableStoreTypes.windowStore[String, (Double, Long)]()
                )
              )

            val tradeVolumes = tradeVolumeStore.fetch(pair, oneHourAgo, now).asScala
            val averagePrices = averagePriceStore.fetch(pair, oneHourAgo, now).asScala

            val totalVolume = tradeVolumes.map(_.value).sum
            val totalPrices = averagePrices.map(_.value._1).sum
            val countPrices = averagePrices.map(_.value._2).sum
            val averagePrice = if (countPrices != 0) totalPrices / countPrices else 0

            Json.obj(
              "pair" -> pair,
              "volume_over_last_hour" -> totalVolume,
              "average_price_over_last_hour" -> averagePrice
            )
          }
        }
      }
    )
  }
}
