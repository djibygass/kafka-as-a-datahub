package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.MeanLatencyForURL

import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * -------------------
 * Part.3 of exercise: Interactive Queries
 * -------------------
 */
object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("visits" / Segment) { period: String =>
        get {
          period match {
            case "30s" =>
              // TODO: load the store containing the visits count of the last 30 seconds and query it to
              // TODO: fetch the keys of the last window and its info
              val kvStore30Seconds: ReadOnlyWindowStore[String, Long] = ???

              complete(
                // TODO: output a list of VisitCountResponse objects
                ???
              )
            case "1m" =>
              // TODO: load the store containing the visits count of the last minute and query it to
              // TODO: fetch the keys of the last window and its info
              val kvStore1Minute: ReadOnlyWindowStore[String, Long] = ???

              complete(
                // TODO: output a list of VisitCountResponse objects
                ???
              )
            case "5m" =>
              // TODO: load the store containing the visits count of the last five minutes and query it to
              // TODO: fetch the keys of the last window and its info
              val kvStore5Minute: ReadOnlyWindowStore[String, Long] = ???

              complete(
                // TODO: output a list of VisitCountResponse objects
                ???
              )
            case _ =>
              // unhandled period asked
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      },
      path("latency" / "beginning") {
        get {
          // TODO: output the mean latency per URL since the start of the app
          val kvStoreMeanLatencyPerURL: ReadOnlyKeyValueStore[String, MeanLatencyForURL] = ???

          complete(
            // TODO: output a list of MeanLatencyForURLResponse objects
            ???
          )
        }
      }
    )
  }
}
