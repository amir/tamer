package tamer.oci.objectstorage

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.job.{AbstractStatefulSourceTamerJob, SourceStateChanged}
import zio.{Chunk, Queue, Ref, Schedule, Task, ZIO}
import zio.ZIO.when
import zio.blocking.Blocking
import zio.clock.Clock
import zio.oci.objectstorage.ObjectStorage

import java.time.Duration
import zio.oci.objectstorage.ListObjectsOptions

class TamerOciObjectStorageJob[
    R <: zio.oci.objectstorage.ObjectStorage with Blocking with Clock with KafkaConfig,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](setup: OciObjectStorageConfiguration[R, K, V, S])
    extends AbstractStatefulSourceTamerJob[R, K, V, S, Names](setup.generic) {
  private final val logTask: Task[LogWriter[Task]]       = log4sFromName.provide("tamer.oci.objectstorage")
  override protected def createInitialSourceState: Names = List.empty

  override protected def createSchedule: Schedule[Any, Any, (Duration, Long)] = Schedule.exponential(
    setup.pollingTimings.minimumIntervalForBucketFetch
  ) || Schedule.spaced(
    setup.pollingTimings.maximumIntervalForBucketFetch
  )

  override protected def updatedSourceState(currentState: Ref[Names], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged] =
    updateListOfNames(currentState, setup.namespace, setup.bucketName, setup.prefix, token)

  private def updateListOfNames(
      namesR: NamesR,
      namespace: String,
      bucketName: String,
      prefix: String,
      namesChangedToken: Queue[Unit]
  ): ZIO[ObjectStorage with Clock, Throwable, SourceStateChanged] =
    for {
      log                  <- logTask
      _                    <- log.info(s"getting list of names in namespace $namespace and bucket $bucketName with prefix $prefix")
      initialObjectListing <- zio.oci.objectstorage.listObjects(namespace, bucketName, ListObjectsOptions(Some(prefix), 1))
      allObjectListing     <- zio.oci.objectstorage.paginateObjects(initialObjectListing).take(3).runCollect.map(_.toList)
      nameList      = allObjectListing.flatMap(_.objectSummaries).map(_.getName)
      cleanNameList = nameList.filter(_.startsWith(prefix))
      previousListOfNames <- namesR.getAndSet(cleanNameList)
      nameListChanged = cleanNameList.sorted != previousListOfNames.sorted
      _ <- when(nameListChanged)(log.debug("Detected change in name list") *> namesChangedToken.offer(()))
    } yield if (nameListChanged) SourceStateChanged(true) else SourceStateChanged(false)

  protected final def iteration(namesR: NamesR, namesChangedToken: Queue[Unit])(
      currentState: S,
      q: Queue[Chunk[(K, V)]]
  ): ZIO[R with ObjectStorage, TamerError, S] =
    (for {
      log       <- logTask
      nextState <- setup.transitions.getNextState(namesR, currentState, namesChangedToken)
      _         <- log.debug(s"Next state computed to be $nextState")
    } yield nextState).mapError(e => TamerError("Error while iterating", e))
}

object TamerOciObjectStorageJob {
  def apply[
      R <: ObjectStorage with Blocking with Clock with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec,
      S <: Product: Codec
  ](
      setup: OciObjectStorageConfiguration[R, K, V, S]
  ) = new TamerOciObjectStorageJob[R, K, V, S](setup)
}
