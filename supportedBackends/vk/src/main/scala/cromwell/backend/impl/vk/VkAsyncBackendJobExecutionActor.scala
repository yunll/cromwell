package cromwell.backend.impl.vk

import java.io.FileNotFoundException
import java.nio.file.{FileAlreadyExistsException}

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.http.scaladsl.model.{RequestEntity, _}
import cats.syntax.apply._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import common.validation.ErrorOr.ErrorOr
import common.validation.Validation._
import cromwell.backend.BackendJobLifecycleActor
import cromwell.backend.async.{AbortedExecutionHandle, ExecutionHandle, FailedNonRetryableExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.core.retry.Retry._
import wom.values.WomFile
import wom.values._
import net.ceedubs.ficus.Ficus._
import common.collections.EnhancedCollections._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import skuber.batch.Job
import skuber.json.batch.format._
import wdl.draft2.model.FullyQualifiedName
import skuber.json.PlayJsonSupportForAkkaHttp._
import cromwell.backend.impl.vk.VkResponseJsonFormatter._

sealed trait VkRunStatus {
  def isTerminal: Boolean
}

case object Running extends VkRunStatus {
  def isTerminal = false
}

case object Complete extends VkRunStatus {
  def isTerminal = true
}

case object FailedOrError extends VkRunStatus {
  def isTerminal = true
}

case object Cancelled extends VkRunStatus {
  def isTerminal = true
}

object VkAsyncBackendJobExecutionActor {
  val JobIdKey = "vk_job_id"
}

class VkAsyncBackendJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams)
  extends BackendJobLifecycleActor with StandardAsyncExecutionActor with VkJobCachingActorHelper {
  implicit val actorSystem = context.system
  implicit val materializer = ActorMaterializer()

  implicit val dispatcher = actorSystem.dispatcher

  override type StandardAsyncRunInfo = Any

  override type StandardAsyncRunState = VkRunStatus

  def statusEquivalentTo(thiz: StandardAsyncRunState)(that: StandardAsyncRunState): Boolean = thiz == that

  override lazy val pollBackOff = SimpleExponentialBackoff(
    initialInterval = 1 seconds,
    maxInterval = 5 minutes,
    multiplier = 1.1
  )

  override lazy val executeOrRecoverBackOff = SimpleExponentialBackoff(
    initialInterval = 3 seconds,
    maxInterval = 30 seconds,
    multiplier = 1.1
  )

  private lazy val realDockerImageUsed: String = jobDescriptor.maybeCallCachingEligible.dockerHash.getOrElse(runtimeAttributes.dockerImage)
  override lazy val dockerImageUsed: Option[String] = Option(realDockerImageUsed)

  private val namespace = vkConfiguration.namespace

  private val apiServerUrl = vkConfiguration.apiServerUrl
  private var token = vkConfiguration.token

  if(token.contains("{{") || token.isEmpty){
    token = "MIISRAYJKoZIhvcNAQcCoIISNTCCEjECAQExDTALBglghkgBZQMEAgEwghBUBgkqhkiG9w0BBwGgghBFBIIQQXsidG9rZW4iOnsiZXhwaXJlc19hdCI6IjIwMTktMDctMDlUMDI6NDQ6MTUuMzgxMDAwWiIsIm1ldGhvZHMiOlsicGFzc3dvcmQiXSwiY2F0YWxvZyI6W10sInJvbGVzIjpbeyJuYW1lIjoidGVfYWRtaW4iLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9jc2JzX3JlcF9hY2NlbGVyYXRpb24iLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9ydHkiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9jc2ciLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9zaXNfcmFzciIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2FzZGZnYXNmIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfYmxhY2tsaXN0IiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfb3BfZ2F0ZWRfdmlzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfYXN2YXN2YSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3VmcyIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX29jZWFubGluayIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3ZpcF9iYW5kd2lkdGgiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9ldnNfZXNzZCIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2lvZHBzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfcmRzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfaGNtcyIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2Nic19xaSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX1Rlc3QwNDE4MDEiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9tZWV0aW5nX2VuZHBvaW50X2J1eSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2psayIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2NiciIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2NzYyIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3Nkd2FudXJsZmlsdGVyIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfc2lzX3R0c19zaXNjIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfZWNzcXVpY2tkZXBsb3kiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9vcF9nYXRlZF9laV9kYXl1X2RsZyIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3hpYW9qdW5fMDUxMF90ZXN0IiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfeGlhb2p1bjA2MTQxIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfZW1haWxiaXRpYW4wMiIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2V2c192b2x1bWVfcmVjeWNsZV9iaW4iLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9vY2VhbmJvb3N0ZXIiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF92Z2l2cyIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2ZkYSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX29wX2dhdGVkX2llZiIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3dlciIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3Rlc3RfaWFtMi4wIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfc2lzX2Fzcl9sb25nIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfaXBzZWN2cG4iLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9laXAiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9tdWx0aV9iaW5kIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfbmxwX2x1X3NhIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfbmxwX210IiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfVGVzdDA0MjgwMSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3hpYW9qdW50ZXN0MTkwMzI2IiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfcHJvamVjdF9kZWwiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF92Z3dzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfVGVzdDA1MDUwMiIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2VpX2RheXVfZGxnIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfbGVnYWN5IiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfc2VydmljZV90aWNrZXRzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfVGVzdDA1MDUwMSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3doaXRlbGlzdCIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2VtYWlsZmVpYml0aWFuIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfZWVlIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfc2lzX2Fzcl9sb25nX3Npc2MiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9ubHBfbGdfdHMiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9zZnN0dXJibyIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2FhYSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX25scF9sdV90YyIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3dlIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfYnMiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF90ZXN0NCIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3Rlc3Q1IiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfdGFzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfdGVzdDMiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9lcHMiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9jc2JzX3Jlc3RvcmVfYWxsIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfVGVzdDA2MjF4aWFvanVuIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfdmFzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfc2VydmljZXN0YWdlX21ncl9hcm0iLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9UZXN0eGlhb2p1bjA3MDIwMSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX3Rlc3Rjb2RlIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfc2RmIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfYXNmYXNmIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfY2hlbmd1b2h1YTEyMTMxMzIiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9kZnMiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9xdWlja2J1eSIsImlkIjoiMCJ9LHsibmFtZSI6Im9wX2dhdGVkX2NwdHNfY2hhb3MiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9oc3NfY2dzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfVEVTVDA2MTR4aWFvanVuIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfZnJlIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfd3MiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9jc2JzX3BlcmlvZGljX3R5cGUiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9URVNUMjAxOTAzMDYiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF92aXMiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9zc2oiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF91cmxmaWx0ZXIiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9zdXBwb3J0X3BsYW4iLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9UZXN0MDQyNTAxIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfbmxwX25scGYiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9yZWYiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF9pZWNzIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfdmd2YXMiLCJpZCI6IjAifSx7Im5hbWUiOiJvcF9nYXRlZF96emNlc2hpIiwiaWQiOiIwIn0seyJuYW1lIjoib3BfZ2F0ZWRfc2lzX2Fzcl9zaG9ydF9zaXNjIiwiaWQiOiIwIn1dLCJwcm9qZWN0Ijp7ImRvbWFpbiI6eyJuYW1lIjoicGFhc19nY3NfYzAwNDI0MTYyXzAxIiwiaWQiOiJmNGYxMGEwNWNlOTY0MzMwOTY3NTA4NTViNDMyOTgxMiJ9LCJuYW1lIjoiY24tbm9ydGgtNyIsImlkIjoiMWYzYWY5ZWMxYjRkNDNkMGI1MWFjNGM0YmVhYjIxNTkifSwiaXNzdWVkX2F0IjoiMjAxOS0wNy0wOFQwMjo0NDoxNS4zODEwMDBaIiwidXNlciI6eyJkb21haW4iOnsibmFtZSI6InBhYXNfZ2NzX2MwMDQyNDE2Ml8wMSIsImlkIjoiZjRmMTBhMDVjZTk2NDMzMDk2NzUwODU1YjQzMjk4MTIifSwibmFtZSI6InBhYXNfZ2NzX2MwMDQyNDE2Ml8wMSIsInBhc3N3b3JkX2V4cGlyZXNfYXQiOiIiLCJpZCI6ImY3NWNhZjZiOTE5MTRiYThhOTg2NTE1N2ZjNTZjZmNhIn19fTGCAcMwggG-AgEBMIGZMIGLMQswCQYDVQQGEwJDTjESMBAGA1UECAwJR3VhbmdEb25nMREwDwYDVQQHDAhTaGVuWmhlbjEuMCwGA1UECgwlSHVhd2VpIFNvZnR3YXJlIFRlY2hub2xvZ2llcyBDby4sIEx0ZDEOMAwGA1UECwwFQ2xvdWQxFTATBgNVBAMMDGNhNjYucGtpLmlhbQIJAPhlgANJhFnwMAsGCWCGSAFlAwQCATANBgkqhkiG9w0BAQEFAASCAQBA0m-OrUVaAUY7d9gYJEXdu7606EUEIOmtBCxX10Vl1rHldlKRw-cKSXWN3Vrr-I3Osv35zwlijqbsz2bWihxpV23eCbxGtb9ksZeT6w6TYA1BohMM01MgfqGcS6WfMt01QqsMzgWak-fNsHZTZqNdit3ssYmwUd6DPseLyLt2EmEQKAGseFyk3bGwRZb9u+HUAStSyG9nad6k6yhOjuORl0TwWLgU7IcjcCHLhwXgm-tbnrK48UJGzuWmI+WnAbLN59DI6Kc2Fo33xN660B7RkbXL9tZrEyD2oonCZjr6Of210v5RnXSUiA2cuwqxfweJjWT-Q+4wInW2uYBVnrrA"
  }

  override lazy val jobTag: String = jobDescriptor.key.tag

  private val outputMode = validate {
    OutputMode.withName(
      configurationDescriptor.backendConfig
        .getAs[String]("output-mode")
        .getOrElse("granular").toUpperCase
    )
  }

  override def mapCommandLineWomFile(womFile: WomFile): WomFile = {
    womFile.mapFile(value =>
      (getPath(value), asAdHocFile(womFile)) match {
        case (Success(path: Path), Some(adHocFile)) =>
          // Ad hoc files will be placed directly at the root ("/cromwell_root/ad_hoc_file.txt") unlike other input files
          // for which the full path is being propagated ("/cromwell_root/path/to/input_file.txt")
          vkJobPaths.containerExec(commandDirectory, adHocFile.alternativeName.getOrElse(path.name))
        case _ => mapCommandLineJobInputWomFile(womFile).value
      }
    )
  }

  override def mapCommandLineJobInputWomFile(womFile: WomFile): WomFile = {
    womFile.mapFile(value =>
      getPath(value) match {
        case Success(path: Path) if path.startsWith(vkJobPaths.workflowPaths.DockerRoot) =>
          path.pathAsString
        case Success(path: Path) if path.equals(vkJobPaths.callExecutionRoot) =>
          commandDirectory.pathAsString
        case Success(path: Path) if path.startsWith(vkJobPaths.callExecutionRoot) =>
          vkJobPaths.containerExec(commandDirectory, path.name)
        case Success(path: Path) if path.startsWith(vkJobPaths.callRoot) =>
          vkJobPaths.callDockerRoot.resolve(path.name).pathAsString
        case Success(path: Path) =>
          vkJobPaths.callInputsDockerRoot.resolve(path.pathWithoutScheme.stripPrefix("/")).pathAsString
        case _ =>
          value
      }
    )
  }

  override lazy val commandDirectory: Path = {
    runtimeAttributes.dockerWorkingDir match {
      case Some(path) => DefaultPathBuilder.get(path)
      case None => vkJobPaths.callExecutionDockerRoot
    }
  }

  def createTaskMessage(): ErrorOr[Job] = {
    val task = (commandScriptContents, outputMode).mapN({
      case (contents, mode) => VkTask(
        jobDescriptor,
        configurationDescriptor,
        jobLogger,
        vkJobPaths,
        runtimeAttributes,
        commandDirectory,
        contents,
        instantiatedCommand,
        realDockerImageUsed,
        mapCommandLineWomFile,
        jobShell,
        mode)
    })

    task.map(task => Job(task.name).withTemplate(task.templateSpec))
  }

  def writeScriptFile(): Future[Unit] = {
    commandScriptContents.fold(
      errors => Future.failed(new RuntimeException(errors.toList.mkString(", "))),
      asyncIo.writeAsync(jobPaths.script, _, Seq.empty)
    )
  }

  private def writeFunctionFiles: Map[FullyQualifiedName, Seq[WomFile]] =
    instantiatedCommand.createdFiles map { f => f.file.value.md5SumShort -> List(f.file) } toMap

  private val callInputFiles: Map[FullyQualifiedName, Seq[WomFile]] = jobDescriptor
    .fullyQualifiedInputs
    .safeMapValues {
      _.collectAsSeq { case w: WomFile => w }
    }

  private def checkInputs() = {
    var copies = List[Future[Unit]]()
    (callInputFiles ++ writeFunctionFiles).flatMap {
      case (_, files) => files.flatMap(_.flattenFiles).zipWithIndex.map {
        case (f, _) =>
          getPath(f.value) match {
            case Success(path: Path) if path.uri.getScheme.equals("obs") =>
              val destination = vkJobPaths.callInputsRoot.resolve(path.pathWithoutScheme.stripPrefix("/"))
              if (!destination.exists) {
                val future = asyncIo.copyAsync(path, destination)
                copies = future :: copies
              }
            case _ =>
              Nil
          }
      }
    }
    for(future <- copies){
      Await.result(future, Duration.Inf)
    }
  }

  override def executeAsync(): Future[ExecutionHandle] = {
    // create call exec dir
    vkJobPaths.callExecutionRoot.createPermissionedDirectories()
    checkInputs()
    val taskMessageFuture = createTaskMessage().fold(
      errors => Future.failed(new RuntimeException(errors.toList.mkString(", "))),
      Future.successful)
    jobLogger.warn("taskMessage: {}", taskMessageFuture)
    for {
      _ <- writeScriptFile()
      taskMessage <- taskMessageFuture
      entity <- Marshal(taskMessage).to[RequestEntity]
      ctr <- makeRequest[Job](HttpRequest(method = HttpMethods.POST,
        headers = List(RawHeader("Content-Type", "application/json"),
          RawHeader("X-Auth-Token", token)),
        uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs",
        entity = entity))
    } yield PendingExecutionHandle(jobDescriptor, StandardAsyncJob(ctr.name), None, previousState = None)
  }

  override def reconnectAsync(jobId: StandardAsyncJob) = {
    val handle = PendingExecutionHandle[StandardAsyncJob, StandardAsyncRunInfo, StandardAsyncRunState](jobDescriptor, jobId, None, previousState = None)
    Future.successful(handle)
  }

  override def recoverAsync(jobId: StandardAsyncJob) = reconnectAsync(jobId)

  override def reconnectToAbortAsync(jobId: StandardAsyncJob) = {
    tryAbort(jobId)
    reconnectAsync(jobId)
  }

  override def tryAbort(job: StandardAsyncJob): Unit = {

    val returnCodeTmp = jobPaths.returnCode.plusExt("kill")
    returnCodeTmp.write(s"$SIGTERM\n")
    try {
      returnCodeTmp.moveTo(jobPaths.returnCode)
    } catch {
      case _: FileAlreadyExistsException =>
        // If the process has already completed, there will be an existing rc file.
        returnCodeTmp.delete(true)
    }
    makeRequest[CancelTaskResponse](HttpRequest(method = HttpMethods.DELETE,
      headers = List(RawHeader("Content-Type", "application/json"),
        RawHeader("X-Auth-Token", token)),
      uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs/${job.jobId}")) onComplete {
      case Success(_) => jobLogger.info("{} Aborted {}", tag: Any, job.jobId)
      case Failure(ex) => jobLogger.warn("{} Failed to abort {}: {}", tag, job.jobId, ex.getMessage)
    }

    ()
  }

  override def requestsAbortAndDiesImmediately: Boolean = false

  override def pollStatusAsync(handle: StandardAsyncPendingExecutionHandle): Future[VkRunStatus] = {
    makeRequest[Job](HttpRequest(headers = List(RawHeader("Content-Type", "application/json"),
      RawHeader("X-Auth-Token", token)),
      uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs/${handle.pendingJob.jobId}")) map {
      response =>
        val state = response.status
        if(state.isEmpty){
          Running
        }else {
          state.get match {
            case s if s.failed.getOrElse(0) > 0 =>
              jobLogger.info(s"VK reported an error for Job ${handle.pendingJob.jobId}: '$s'")
              FailedOrError

            case s if s.succeeded.getOrElse(0) > 0 =>
              jobLogger.info(s"Job ${handle.pendingJob.jobId} is complete")
              Complete

            case s if s.active.getOrElse(0) == 0 =>
              jobLogger.info(s"Job ${handle.pendingJob.jobId} was canceled")
              Cancelled

            case _ => Running
          }
        }
    }
  }

  override def customPollStatusFailure: PartialFunction[(ExecutionHandle, Exception), ExecutionHandle] = {
    case (oldHandle: StandardAsyncPendingExecutionHandle@unchecked, e: Exception) =>
      jobLogger.error(s"$tag VK Job ${oldHandle.pendingJob.jobId} has not been found, failing call")
      FailedNonRetryableExecutionHandle(e)
  }

  override def handleExecutionFailure(status: StandardAsyncRunState, returnCode: Option[Int]) = {
    status match {
      case Cancelled => Future.successful(AbortedExecutionHandle)
      case _ => super.handleExecutionFailure(status, returnCode)
    }
  }

  override def isTerminal(runStatus: VkRunStatus): Boolean = {
    runStatus.isTerminal
  }

  override def isDone(runStatus: VkRunStatus): Boolean = {
    runStatus match {
      case Complete => true
      case _ => false
    }
  }

  override def mapOutputWomFile(womFile: WomFile): WomFile = {
    womFile mapFile { path =>
      val absPath = getPath(path) match {
        case Success(absoluteOutputPath) if absoluteOutputPath.isAbsolute => absoluteOutputPath
        case _ => vkJobPaths.callExecutionRoot.resolve(path)
      }
      if (!absPath.exists) {
        throw new FileNotFoundException(s"Could not process output, file not found: ${absPath.pathAsString}")
      } else {
        syncOutput(absPath)
        absPath.pathAsString
      }
    }
  }

  private def syncOutput(path: Path) = {
    if(!vkConfiguration.storagePath.isEmpty) {
      val destPath = getPath(path.pathAsString.replace(vkJobPaths.workflowPaths.executionRoot.pathAsString, vkConfiguration.storagePath.get)).get
      asyncIo.copyAsync(path, destPath)
    }
  }

  private def makeRequest[A](request: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, A]): Future[A] = {
    for {
      response <- withRetry(() => Http().singleRequest(request))
      data <- if (response.status.isFailure()) {
        response.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String) flatMap { errorBody =>
          Future.failed(new RuntimeException(s"Failed VK request: Code ${response.status.intValue()}, Body = $errorBody"))
        }
      } else {
        Unmarshal(response.entity).to[A]
      }
    } yield data
  }
}
