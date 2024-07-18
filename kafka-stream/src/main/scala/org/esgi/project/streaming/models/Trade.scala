package org.esgi.project.streaming.models

import play.api.libs.json._

case class Trade(
    e: String, // Event type
    E: Long, // Event time
    s: String, // Symbol
    t: Long, // Trade ID
    p: String, // Price
    q: String, // Quantity
    b: Long, // Buyer order ID
    a: Long, // Seller order ID
    T: Long, // Trade time
    m: Boolean, // Is the buyer the market maker?
    M: Boolean // Ignore
)

object Trade {
  implicit val tradeFormat: Format[Trade] = Json.format[Trade]
}
