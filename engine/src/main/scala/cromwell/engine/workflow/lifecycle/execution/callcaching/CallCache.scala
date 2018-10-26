package cromwell.engine.workflow.lifecycle.execution.callcaching

import akka.actor.ActorRef
import cats.data.NonEmptyList
import common.util.StringUtil._
import cromwell.backend.BackendJobDescriptorKey
import cromwell.backend.BackendJobExecutionActor.{CallCached, JobFailedNonRetryableResponse, JobSucceededResponse}
import cromwell.core.ExecutionIndex.{ExecutionIndex, IndexEnhancedIndex}
import cromwell.core.callcaching.{HashKey, HashResult, HashValue}
import cromwell.core.path.{Path, PathBuilder}
import cromwell.core.simpleton.WomValueSimpleton._
import cromwell.core.simpleton.{WomValueBuilder, WomValueSimpleton}
import cromwell.core.{CallOutputs, WorkflowId}
import cromwell.database.sql.SqlConverters._
import cromwell.database.sql._
import cromwell.database.sql.joins.CallCachingJoin
import cromwell.database.sql.tables._
import cromwell.engine.workflow.lifecycle.execution.callcaching.CallCache._
import cromwell.engine.workflow.lifecycle.execution.callcaching.CallCacheReadActor.AggregatedCallHashes
import cromwell.engine.workflow.lifecycle.execution.callcaching.EngineJobHashingActor.CallCacheHashes
import cromwell.server.CCPrefixQueryFileWrite
import cromwell.services.instrumentation.CromwellInstrumentation.InstrumentationPath
import cromwell.services.instrumentation.CromwellInstrumentation
import wom.core._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

final case class CallCachingEntryId(id: Int)
/**
  * Given a database-layer CallCacheStore, this accessor can access the database with engine-friendly data types.
  */
class CallCache(database: CallCachingSqlDatabase, override val serviceRegistryActor: ActorRef) extends CromwellInstrumentation{
  val hasHashMatchPath: InstrumentationPath = NonEmptyList.of("hasHashMatch")
  val callCachingHitPath: InstrumentationPath = NonEmptyList.of("callCachingHit")

  val hasHashMatchFilePath: String = "hasHashMatchPrefixQueryTime.txt"
  val ccHitFilePath: String = "callCacheHitPrefixQueryTime.txt"


  def addToCache(bundles: Seq[CallCacheHashBundle], batchSize: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    val joins = bundles map { b =>
      val metaInfo = CallCachingEntry(
        workflowExecutionUuid = b.workflowId.toString,
        callFullyQualifiedName = b.fullyQualifiedName,
        jobIndex = b.jobIndex.fromIndex,
        jobAttempt = b.jobAttempt,
        returnCode = b.returnCode,
        allowResultReuse = b.allowResultReuse)
      val result = b.callOutputs.outputs.simplify
      val jobDetritus = b.jobDetritusFiles.getOrElse(Map.empty)
      buildCallCachingJoin(metaInfo, b.callCacheHashes, result, jobDetritus)
    }

    database.addCallCaching(joins, batchSize)
  }

  private def buildCallCachingJoin(callCachingEntry: CallCachingEntry, callCacheHashes: CallCacheHashes,
                                   result: Iterable[WomValueSimpleton], jobDetritus: Map[String, Path]): CallCachingJoin = {

    val hashesToInsert: Iterable[CallCachingHashEntry] = {
      callCacheHashes.hashes map { hash => CallCachingHashEntry(hash.hashKey.key, hash.hashValue.value) }
    }

    val aggregatedHashesToInsert: Option[CallCachingAggregationEntry] = {
      Option(CallCachingAggregationEntry(
        baseAggregation = callCacheHashes.aggregatedInitialHash,
        inputFilesAggregation = callCacheHashes.fileHashes.map(_.aggregatedHash)
      ))
    }

    val resultToInsert: Iterable[CallCachingSimpletonEntry] = {
      result map {
        case WomValueSimpleton(simpletonKey, wdlPrimitive) =>
          CallCachingSimpletonEntry(simpletonKey, wdlPrimitive.valueString.toClobOption, wdlPrimitive.womType.toDisplayString)
      }
    }

    val jobDetritusToInsert: Iterable[CallCachingDetritusEntry] = {
      jobDetritus map {
        case (fileName, filePath) => CallCachingDetritusEntry(fileName, filePath.pathAsString.toClobOption)
      }
    }

    CallCachingJoin(callCachingEntry, hashesToInsert.toSeq, aggregatedHashesToInsert, resultToInsert.toSeq, jobDetritusToInsert.toSeq)
  }

  def hasBaseAggregatedHashMatch(baseAggregatedHash: String, hints: List[CacheHitHint])(implicit ec: ExecutionContext): Future[(Boolean, FiniteDuration)] = {
    val ccpp = hints collectFirst { case h: CallCachePathPrefixes => h.prefixes }

    val start = System.currentTimeMillis
    val future = database.hasMatchingCallCachingEntriesForBaseAggregation(baseAggregatedHash, ccpp)

    future.map{bool =>
      val totalTime = (System.currentTimeMillis - start).millis
      CCPrefixQueryFileWrite.writePrefixQueryTimeToFile(hasHashMatchFilePath, totalTime.toMillis.toString)
      sendTiming(hasHashMatchPath, totalTime, Option("cc-prefix-query"))
      (bool, totalTime)
    }

//    val a = future.onComplete {
//      case Success(bool) => {
//        val totalTime = (System.currentTimeMillis - start).millis
//        println(s"------------- FIND ME -------------")
//        println(s"Method: hasBaseAggregatedHashMatch")
//        println(s"baseAggregatedHash: $baseAggregatedHash")
//        println(s"hintsSize: ${ccpp.map(l => l.size)}")
//        println(s"Found match?: $bool")
//        println(s"*** Time spent: $totalTime")
//        println(s"----------------------------------")
//
//        Future.successful(bool, totalTime)
//      }
//      case Failure(e) => Future.failed(e)
//    }
  }

  def callCachingHitForAggregatedHashes(aggregatedCallHashes: AggregatedCallHashes, prefixesHint: Option[CallCachePathPrefixes], hitNumber: Int)
                                       (implicit ec: ExecutionContext): Future[(Option[CallCachingEntryId], FiniteDuration)] = {
    val start = System.currentTimeMillis

    val future = database.findCacheHitForAggregation(
      baseAggregationHash = aggregatedCallHashes.baseAggregatedHash,
      inputFilesAggregationHash = aggregatedCallHashes.inputFilesAggregatedHash,
      callCachePathPrefixes = prefixesHint.map(_.prefixes),
      hitNumber).map(_ map CallCachingEntryId.apply)

//    future.onComplete {
//      case Success(idOption) => {
//        val totalTime = (System.currentTimeMillis - start).millis
//        println(s"------------- FIND ME -------------")
//        println(s"Method: callCachingHitForAggregatedHashes")
//        println(s"aggregatedCallHashes: $aggregatedCallHashes")
//        println(s"baseAggregationHash: ${aggregatedCallHashes.baseAggregatedHash}")
//        println(s"inputFilesAggregationHash: ${aggregatedCallHashes.inputFilesAggregatedHash}")
//        println(s"hintsSize: ${prefixesHint.map(_.prefixes).get.size}")
//        println(s"hitNumber: $hitNumber")
//        idOption.foreach(id => println(s"CallCachingEntryId returned: $id"))
//        println(s"*** Time spent: $totalTime")
//        println(s"----------------------------------")
//      }
//      case Failure(e) => {
//        println(s"------------- FIND ME -------------")
//        println(s"Something went wrong! Error: ${e.getMessage}")
//        println(s"----------------------------------")
//      }
//    }
//
//    future

    future.map{ id =>
      val totalTime = (System.currentTimeMillis - start).millis
      CCPrefixQueryFileWrite.writePrefixQueryTimeToFile(ccHitFilePath, totalTime.toMillis.toString)
      sendTiming(callCachingHitPath, totalTime, Option("cc-prefix-query"))
      (id, totalTime)
    }
  }

  def fetchCachedResult(callCachingEntryId: CallCachingEntryId)(implicit ec: ExecutionContext): Future[Option[CallCachingJoin]] = {
    database.queryResultsForCacheId(callCachingEntryId.id)
  }

  def callCachingJoinForCall(workflowUuid: String, callFqn: String, index: Int)(implicit ec: ExecutionContext): Future[Option[CallCachingJoin]] = {
    database.callCacheJoinForCall(workflowUuid, callFqn, index)
  }

  def invalidate(callCachingEntryId: CallCachingEntryId)(implicit ec: ExecutionContext) = {
    database.invalidateCall(callCachingEntryId.id)
  }
}

object CallCache {
  object CallCacheHashBundle {
    def apply(workflowId: WorkflowId, callCacheHashes: CallCacheHashes, jobSucceededResponse: JobSucceededResponse) = {
      new CallCacheHashBundle(
        workflowId = workflowId,
        callCacheHashes = callCacheHashes,
        fullyQualifiedName = jobSucceededResponse.jobKey.call.fullyQualifiedName,
        jobIndex = jobSucceededResponse.jobKey.index,
        jobAttempt = Option(jobSucceededResponse.jobKey.attempt),
        returnCode = jobSucceededResponse.returnCode,
        allowResultReuse = true,
        callOutputs = jobSucceededResponse.jobOutputs,
        jobDetritusFiles = jobSucceededResponse.jobDetritusFiles
      )
    }

    def apply(workflowId: WorkflowId, callCacheHashes: CallCacheHashes, jobFailedNonRetryableResponse: JobFailedNonRetryableResponse) = {
      new CallCacheHashBundle(
        workflowId = workflowId,
        callCacheHashes = callCacheHashes,
        fullyQualifiedName = jobFailedNonRetryableResponse.jobKey.node.fullyQualifiedName,
        jobIndex = jobFailedNonRetryableResponse.jobKey.index,
        jobAttempt = Option(jobFailedNonRetryableResponse.jobKey.attempt),
        returnCode = None,
        allowResultReuse = false,
        callOutputs = CallOutputs.empty,
        jobDetritusFiles = None
      )
    }
  }
  case class CallCacheHashBundle private (
                                           workflowId: WorkflowId,
                                           callCacheHashes: CallCacheHashes,
                                           fullyQualifiedName: FullyQualifiedName,
                                           jobIndex: ExecutionIndex,
                                           jobAttempt: Option[Int],
                                           returnCode: Option[Int],
                                           allowResultReuse: Boolean,
                                           callOutputs: CallOutputs,
                                           jobDetritusFiles: Option[Map[String, Path]]
                                         )

  implicit class EnhancedCallCachingJoin(val callCachingJoin: CallCachingJoin) extends AnyVal {
    def toJobSuccess(key: BackendJobDescriptorKey, pathBuilders: List[PathBuilder]): JobSucceededResponse = {
      import cromwell.Simpletons._
      import cromwell.core.path.PathFactory._
      val detritus = callCachingJoin.callCachingDetritusEntries.map({ jobDetritusEntry =>
        jobDetritusEntry.detritusKey -> buildPath(jobDetritusEntry.detritusValue.toRawString, pathBuilders)
      }).toMap

      val outputs = if (callCachingJoin.callCachingSimpletonEntries.isEmpty) CallOutputs(Map.empty)
      else WomValueBuilder.toJobOutputs(key.call.outputPorts, callCachingJoin.callCachingSimpletonEntries map toSimpleton)

      JobSucceededResponse(key, callCachingJoin.callCachingEntry.returnCode,outputs, Option(detritus), Seq.empty, None, resultGenerationMode = CallCached)
    }

    def callCacheHashes: Set[HashResult] = {
      val hashResults = callCachingJoin.callCachingHashEntries.map({
        case CallCachingHashEntry(k, v, _, _) => HashResult(HashKey.deserialize(k), HashValue(v))
      }) ++ callCachingJoin.callCachingAggregationEntry.collect({
        case CallCachingAggregationEntry(k, Some(v), _, _) => HashResult(HashKey.deserialize(k), HashValue(v))
      })

      hashResults.toSet
    }
  }

  sealed trait CacheHitHint
  case class CallCachePathPrefixes(callCacheRootPrefix: Option[String], workflowOptionPrefixes: List[String]) extends CacheHitHint {
    lazy val prefixes: List[String] = (callCacheRootPrefix.toList ++ workflowOptionPrefixes) map { _.ensureSlashed }
  }
}
