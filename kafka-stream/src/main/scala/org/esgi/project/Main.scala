import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.streams.KafkaStreams
import org.esgi.project.api.WebServer
import org.slf4j.{Logger, LoggerFactory}
import org.esgi.project.streaming.StreamProcessing

import scala.concurrent.ExecutionContextExecutor

object Main {
  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    val streams: KafkaStreams = StreamProcessing.run()

    startServer(streams)
    logger.info(s"App started")
  }

  def startServer(streams: KafkaStreams): Unit = {
    Http()
      .newServerAt("0.0.0.0", 8080)
      .bindFlow(WebServer.routes(streams))
    logger.info(s"Server started on port 8080")
  }
}
