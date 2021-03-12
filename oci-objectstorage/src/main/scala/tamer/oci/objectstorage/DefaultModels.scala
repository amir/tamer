package tamer
package objectstorage

import java.time.{Instant, ZoneId}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

final case class Line(str: String)
object Line {
  implicit val codec = AvroCodec.codec[Line]
}

final case class LastProcessedInstant(instant: Instant)
object LastProcessedInstant {
  implicit val codec = AvroCodec.codec[LastProcessedInstant]
}

case class ZonedDateTimeFormatter private (value: DateTimeFormatter)
object ZonedDateTimeFormatter {
  private def apply(value: DateTimeFormatter): ZonedDateTimeFormatter = new ZonedDateTimeFormatter(value)

  def apply(dateTimeFormatter: DateTimeFormatter, zoneId: ZoneId): ZonedDateTimeFormatter =
    apply(dateTimeFormatter.withZone(zoneId))

  def fromPattern(pattern: String, zoneId: ZoneId): ZonedDateTimeFormatter =
    apply(new DateTimeFormatterBuilder().appendPattern(pattern).parseDefaulting(ChronoField.NANO_OF_DAY, 0).toFormatter, zoneId)
}
