package tamer.oci.objectstorage

import com.sksamuel.avro4s.{Codec, SchemaFor}
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.objectstorage.{LastProcessedInstant, Line, ZonedDateTimeFormatter}
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.oci.objectstorage.ObjectStorage
import zio.stream.{Transducer, ZTransducer}

import java.time.ZoneId
import java.time.format.DateTimeFormatter

class TamerOciObjectStorageSuffixDateFetcher[R <: Blocking with Clock with ObjectStorage with KafkaConfig] {
  def fetchAccordingToSuffixDate[
      K <: Product: Codec: SchemaFor,
      V <: Product: Codec: SchemaFor
  ](
      namespace: String,
      bucketName: String,
      prefix: String,
      afterwards: LastProcessedInstant,
      context: TamerOciObjectStorageSuffixDateFetcher.Context[R, K, V]
  ): ZIO[R, TamerError, Unit] = {
    val setup = OciObjectStorageConfiguration.mkTimeBased[R, K, V](namespace, bucketName, prefix, afterwards, context)

    TamerOciObjectStorageJob(setup).fetch()
  }
}

object TamerOciObjectStorageSuffixDateFetcher {
  private val defaultTransducer: Transducer[Nothing, Byte, Line] =
    ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line.apply)

  case class Context[R, K, V](
      deriveKafkaKey: (LastProcessedInstant, V) => K = (l: LastProcessedInstant, _: V) => l,
      transducer: ZTransducer[R, TamerError, Byte, V] = defaultTransducer,
      dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
      pollingTimings: OciObjectStorageConfiguration.PollingTimings = OciObjectStorageConfiguration.PollingTimings(
        minimumIntervalForBucketFetch = 5.minutes,
        maximumIntervalForBucketFetch = 5.minutes
      )
  )
}
