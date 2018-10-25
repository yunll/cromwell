package cromwell.engine.workflow.lifecycle.execution.callcaching

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, LoggingFSM, Props}
import cromwell.core.Dispatcher.EngineDispatcher
import cromwell.core.callcaching.HashingFailedMessage
import cromwell.engine.workflow.lifecycle.execution.callcaching.CallCache.CallCachePathPrefixes
import cromwell.engine.workflow.lifecycle.execution.callcaching.CallCacheHashingJobActor.{CompleteFileHashingResult, InitialHashingResult, NextBatchOfFileHashesRequest, NoFileHashesResult}
import cromwell.engine.workflow.lifecycle.execution.callcaching.CallCacheReadActor._
import cromwell.engine.workflow.lifecycle.execution.callcaching.CallCacheReadingJobActor._
import cromwell.engine.workflow.lifecycle.execution.callcaching.EngineJobHashingActor.{CacheHit, CacheMiss, HashError}

import scala.concurrent.duration.FiniteDuration

/**
  * Receives hashes from the CallCacheHashingJobActor and makes requests to the database to determine whether or not there might be a hit
  * for this job.
  * 
  * First receives the initial hashes, and asks the database if there is at least one entry with the same aggregated initial hash.
  * If not, it's a CacheMiss.
  * If yes, ask the CallCacheHashingJobActor for the next batch of file hashes.
  * Every time a new batch of hashes is received, check against the database if at least one entry matches all those hashes.
  * Keep asking for new batches until either one returns no matching entry, in which case it's a CacheMiss, or until it receives
  * the last batch along with the aggregated file hash.
  * In the latter case, asks the database for the first entry matching both the initial and aggregated file hash (if any).
  * Sends the response to its parent.
  * In case of a CacheHit, stays alive in case using the hit fails and it needs to fetch the next one. Otherwise just dies.
  */
class CallCacheReadingJobActor(callCacheReadActor: ActorRef, prefixesHint: Option[CallCachePathPrefixes]) extends LoggingFSM[CallCacheReadingJobActorState, CCRJAData] {
  var hasMatchMin: FiniteDuration = FiniteDuration(100, TimeUnit.SECONDS)
  var hasMatchMax: FiniteDuration = FiniteDuration(0, TimeUnit.SECONDS)
  var hasMatchTotal: FiniteDuration = FiniteDuration(0, TimeUnit.SECONDS)
  var hasMatchCt: Long = 0
  var hasMatchAvg: FiniteDuration = FiniteDuration(0, TimeUnit.SECONDS)

  var ccHitMin: FiniteDuration = FiniteDuration(100, TimeUnit.SECONDS)
  var ccHitMax: FiniteDuration = FiniteDuration(0, TimeUnit.SECONDS)
  var ccHitTotal: FiniteDuration = FiniteDuration(0, TimeUnit.SECONDS)
  var ccHitCt: Long = 0
  var ccHitAvg: FiniteDuration = FiniteDuration(0, TimeUnit.SECONDS)

  def updateHasMatchStats(execTime: FiniteDuration): Unit = {
    hasMatchTotal += execTime
    hasMatchCt += 1
    hasMatchMin = hasMatchMin.min(execTime)
    hasMatchMax = hasMatchMax.max(execTime)
  }

  def updateCCHitStats(execTime: FiniteDuration): Unit = {
    ccHitTotal += execTime
    ccHitCt += 1
    ccHitMin = hasMatchMin.min(execTime)
    ccHitMax = hasMatchMax.max(execTime)
  }

  def printCachingStats(): Unit = {
    println(s"------------- FIND ME OR I WILL FIND YOU! -------------")
    println("hasBaseAggregatedHashMatch")
    println(s"Total time: $hasMatchTotal")
    println(s"#times method was called: $hasMatchCt")
    println(s"Average time: ${hasMatchTotal.div(hasMatchCt)}")
    println(s"Min time: $hasMatchMin")
    println(s"Max time: $hasMatchMax")
    println("**************")
    println("callCachingHitForAggregatedHashes")
    println(s"Total time: $ccHitTotal")
    println(s"#times method was called: $ccHitCt")
    if(ccHitCt > 0) println(s"Average time: ${ccHitTotal.div(ccHitCt)}")
    println(s"Min time: $ccHitMin")
    println(s"Max time: $ccHitMax")
    println(s"-------------------------------------------------------")
  }
  
  startWith(WaitingForInitialHash, CCRJANoData)
  
  when(WaitingForInitialHash) {
    case Event(InitialHashingResult(_, aggregatedBaseHash, hints), CCRJANoData) =>
      callCacheReadActor ! HasMatchingInitialHashLookup(aggregatedBaseHash, hints)
      goto(WaitingForHashCheck) using CCRJAWithData(sender(), aggregatedBaseHash, None, 1)
  }
  
  when(WaitingForHashCheck) {
    case Event(HasMatchingEntries(execTime), CCRJAWithData(hashingActor, _, _, _)) =>
      updateHasMatchStats(execTime)
      hashingActor ! NextBatchOfFileHashesRequest
      goto(WaitingForFileHashes)
    case Event(NoMatchingEntries(execTime), _) =>
      updateHasMatchStats(execTime)
      cacheMiss
  }
  
  when(WaitingForFileHashes) {
    case Event(CompleteFileHashingResult(_, aggregatedFileHash), data: CCRJAWithData) =>
      callCacheReadActor ! CacheLookupRequest(AggregatedCallHashes(data.initialHash, aggregatedFileHash), data.currentHitNumber, prefixesHint)
      goto(WaitingForCacheHitOrMiss) using data.withFileHash(aggregatedFileHash)
    case Event(NoFileHashesResult, data: CCRJAWithData) =>
      callCacheReadActor ! CacheLookupRequest(AggregatedCallHashes(data.initialHash, None), data.currentHitNumber, prefixesHint)
      goto(WaitingForCacheHitOrMiss)
  }
  
  when(WaitingForCacheHitOrMiss) {
    case Event(CacheLookupNextHit(hit, execTime), data: CCRJAWithData) =>
      updateCCHitStats(execTime)
      context.parent ! CacheHit(hit)
      stay() using data.increment
    case Event(CacheLookupNoHit(execTime), _) =>
      updateCCHitStats(execTime)
      cacheMiss
    case Event(NextHit, CCRJAWithData(_, aggregatedInitialHash, aggregatedFileHash, currentHitNumber)) =>
      callCacheReadActor ! CacheLookupRequest(AggregatedCallHashes(aggregatedInitialHash, aggregatedFileHash), currentHitNumber, prefixesHint)
      stay()
  }
  
  whenUnhandled {
    case Event(_: HashingFailedMessage, _) =>
      // No need to send to the parent since it also receives file hash updates
      cacheMiss
    case Event(CacheResultLookupFailure(failure), _) =>
      context.parent ! HashError(failure)
      cacheMiss
  }
  
  def cacheMiss = {
    printCachingStats()
    context.parent ! CacheMiss
    context stop self
    stay()
  }
}

object CallCacheReadingJobActor {
  
  def props(callCacheReadActor: ActorRef, prefixesHint: Option[CallCachePathPrefixes]) = {
    Props(new CallCacheReadingJobActor(callCacheReadActor, prefixesHint)).withDispatcher(EngineDispatcher)
  }
  
  sealed trait CallCacheReadingJobActorState
  case object WaitingForInitialHash extends CallCacheReadingJobActorState
  case object WaitingForHashCheck extends CallCacheReadingJobActorState
  case object WaitingForFileHashes extends CallCacheReadingJobActorState
  case object WaitingForCacheHitOrMiss extends CallCacheReadingJobActorState

  sealed trait CCRJAData
  case object CCRJANoData extends CCRJAData
  case class CCRJAWithData(hashingActor: ActorRef, initialHash: String, fileHash: Option[String], currentHitNumber: Int) extends CCRJAData {
    def increment = this.copy(currentHitNumber = currentHitNumber + 1)
    def withFileHash(aggregatedFileHash: String) = this.copy(fileHash = Option(aggregatedFileHash))
  }

  case object NextHit
}
