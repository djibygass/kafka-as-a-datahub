package org.esgi.project.streaming.models

import play.api.libs.json._

case class Candle(open: Double, close: Double, low: Double, high: Double, volume: Double)

object Candle {
  implicit val candleFormat: Format[Candle] = Json.format[Candle]
}
