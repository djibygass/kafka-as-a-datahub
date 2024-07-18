import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.api.WebServer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.concurrent.ExecutionContextExecutor

object Main {
  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    // Kafka Streams configuration
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("application.id", "my-streams-app")
    props.put("default.key.serde", Serdes.String().getClass.getName)
    props.put("default.value.serde", Serdes.String().getClass.getName)

      val builder = new StreamsBuilder()
      val inputTopic = "trades"

      val stream: KStream[String, String] = builder.stream[String, String](inputTopic)

      stream.foreach((key, value) => {
        println(s"Received record: key = $key, value = $value")
        // Aquí puedes agregar la lógica de procesamiento de tu stream
      })

      val topology = builder.build()
      val streams = new KafkaStreams(topology, props)

      // Iniciar Kafka Streams
      streams.start()

    // Iniciar servidor HTTP con las rutas definidas en WebServer
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
