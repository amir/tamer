package tamer.oci.objectstorage

import com.sksamuel.avro4s.{Codec, SchemaFor}
import tamer.{SourceConfiguration, TamerError}
import tamer.objectstorage.{LastProcessedInstant, ZonedDateTimeFormatter}
import zio.{Queue, UIO, ZIO}
import zio.stream.ZTransducer

import java.time.{Duration, Instant}
import java.time.format.DateTimeFormatter

final case class OciObjectStorageConfiguration[
    R,
    K <: Product: Codec: SchemaFor,
    V <: Product: Codec: SchemaFor,
    S <: Product: Codec: SchemaFor
](
    namespace: String,
    bucketName: String,
    prefix: String,
    tamerStateKafkaRecordKey: Int,
    transducer: ZTransducer[R, TamerError, Byte, V],
    pollingTimings: OciObjectStorageConfiguration.PollingTimings,
    transitions: OciObjectStorageConfiguration.State[K, V, S]
) {
  val generic: SourceConfiguration[K, V, S] = SourceConfiguration[K, V, S](
    SourceConfiguration.SourceSerde[K, V, S](),
    defaultState = transitions.initialState,
    tamerStateKafkaRecordKey = tamerStateKafkaRecordKey
  )
}

object OciObjectStorageConfiguration {
  case class PollingTimings(minimumIntervalForBucketFetch: Duration, maximumIntervalForBucketFetch: Duration)

  case class State[K, V, S](
      initialState: S,
      getNextState: (NamesR, S, Queue[Unit]) => UIO[S],
      deriveKafkaRecordKey: (S, V) => K,
      selectObjectForState: (S, Names) => Option[String]
  )

  private final def suffixWithoutFileExtension(name: String, prefix: String, dateTimeFormatter: DateTimeFormatter): String = {
    val dotCountInDate = dateTimeFormatter.format(Instant.EPOCH).count(_ == '.')
    val nameWithoutExtension =
      if (name.count(_ == '.') > dotCountInDate) name.split('.').splitAt(dotCountInDate + 1)._1.mkString(".") else name
    nameWithoutExtension.stripPrefix(prefix)
  }

  private final def parseInstantFromName(name: String, prefix: String, dateTimeFormatter: DateTimeFormatter): Instant =
    Instant.from(dateTimeFormatter.parse(suffixWithoutFileExtension(name, prefix, dateTimeFormatter)))

  private final def getNextInstant(
      namesR: NamesR,
      afterwards: LastProcessedInstant,
      prefix: String,
      dateTimeFormatter: DateTimeFormatter
  ): ZIO[Any, Nothing, Option[Instant]] = namesR.get.map { names =>
    val sortedFileDates = names
      .map(name => parseInstantFromName(name, prefix, dateTimeFormatter))
      .filter(_.isAfter(afterwards.instant))
      .sorted

    sortedFileDates.headOption
  }

  private final def getNextState(
      prefix: String,
      dateTimeFormatter: DateTimeFormatter
  )(namesR: NamesR, afterwards: LastProcessedInstant, namesChangedToken: Queue[Unit]): UIO[LastProcessedInstant] = {
    val retryAfterWaitingForNameListChange =
      namesChangedToken.take *> getNextState(prefix, dateTimeFormatter)(namesR, afterwards, namesChangedToken)
    getNextInstant(namesR, afterwards, prefix, dateTimeFormatter)
      .flatMap {
        case Some(newInstant) if newInstant.isAfter(afterwards.instant) => UIO(LastProcessedInstant(newInstant))
        case _                                                          => retryAfterWaitingForNameListChange
      }
  }

  private final def selectObjectForInstant(
      zonedDateTimeFormatter: ZonedDateTimeFormatter
  )(lastProcessedInstant: LastProcessedInstant, names: Names): Option[String] = ???

  final def mkTimeBased[R, K <: Product: Codec: SchemaFor, V <: Product: Codec: SchemaFor](
      namespace: String,
      bucketName: String,
      filePathPrefix: String,
      afterwards: LastProcessedInstant,
      context: TamerOciObjectStorageSuffixDateFetcher.Context[R, K, V]
  ): OciObjectStorageConfiguration[R, K, V, LastProcessedInstant] =
    OciObjectStorageConfiguration[R, K, V, LastProcessedInstant](
      namespace,
      bucketName,
      filePathPrefix,
      afterwards.instant.getEpochSecond.intValue,
      context.transducer,
      context.pollingTimings,
      State(
        afterwards,
        getNextState(filePathPrefix, context.dateTimeFormatter.value),
        context.deriveKafkaKey,
        selectObjectForInstant(context.dateTimeFormatter)
      )
    )
}
