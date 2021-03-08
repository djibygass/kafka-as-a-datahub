package org.esgi.project.kafka.models

import play.api.libs.json._

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

case class ConnectionEvent(
                            _id: String,
                            firstName: String,
                            lastName: String,
                            timestamp: OffsetDateTime
                          )

object ConnectionEvent {
  implicit val dateFormattingReads: Reads[OffsetDateTime] = Reads[OffsetDateTime] {
    _.validate[String].flatMap { isoFormattedString =>
      Try(DateTimeFormatter.ISO_DATE_TIME.parse(isoFormattedString))
        .map(r => JsSuccess(OffsetDateTime.from(r)))
        .getOrElse(JsError("Couldn't parse date format"))
    }
  }

  implicit val dateFormattingWrites: Writes[OffsetDateTime] = Writes[OffsetDateTime] { date =>
    JsString(DateTimeFormatter.ISO_DATE_TIME.format(date))
  }

  implicit val format: OFormat[ConnectionEvent] = Json.format[ConnectionEvent]
}
