package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class Candle(date: String, open: Double, close: Double, lowest: Double, highest: Double, volume: Double)

object Candle {
  implicit val format: OFormat[Candle] = Json.format[Candle]
}
