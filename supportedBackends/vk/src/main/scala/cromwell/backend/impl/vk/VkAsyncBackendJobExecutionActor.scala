package cromwell.backend.impl.vk

import java.io.FileNotFoundException
import java.nio.file.FileAlreadyExistsException
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.security.MessageDigest
import java.math.BigInteger
import java.util.Date

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
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
import cromwell.backend.async.{AbortedExecutionHandle, ExecutionHandle, FailedNonRetryableExecutionHandle, FailedRetryableExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.core.retry.Retry._
import wom.values.WomFile
import wom.values._
import net.ceedubs.ficus.Ficus._
import common.collections.EnhancedCollections._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import skuber.batch.Job
import skuber.json.batch.format._
import wdl.draft2.model.FullyQualifiedName
import skuber.json.PlayJsonSupportForAkkaHttp._
import cromwell.backend.impl.vk.VkResponseJsonFormatter._
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients

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

  private val apiServerUrl = s"https://cci.${vkConfiguration.region}.myhuaweicloud.com"
  private val token = initToken()

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
      case (_, _) => VkTask(
        jobDescriptor,
        configurationDescriptor,
        vkJobPaths,
        runtimeAttributes,
        commandDirectory,
        realDockerImageUsed,
        jobShell)
    })

    task.map(task => Job(task.name).withTemplate(task.templateSpec))
  }

  def getHmacSH256(key: Array[Byte], content: Array[Byte]): Array[Byte] = {
    val secret = new SecretKeySpec(key, "HmacSHA256")
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(secret)
    mac.doFinal(content)
  }

  def initToken(): String = {
    Await.result[String](withRetry(() => Future.fromTry(Try(getToken())), maxRetries = runtimeAttributes.maxRetries), Duration.Inf)
  }

  def getToken(): String = {
    // get UTC time
    val timeZone = TimeZone.getTimeZone("GMT");
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val shortSimpleDateFormat = new SimpleDateFormat("HHmmss")
    simpleDateFormat.setTimeZone(timeZone)
    shortSimpleDateFormat.setTimeZone(timeZone)
    val date = new Date()
    val time = simpleDateFormat.format(date)
    val shortTime = shortSimpleDateFormat.format(date)
    val signTime = time + "T" + shortTime + "Z"

    val hwsDate = "X-Hws-Date"
    val region = ""
    val service = ""
    val terminalString = "hws_request"
    val authHeaderPrefix = "HWS-HMAC-SHA256"
    val hwsName = "HWS"
    val query = ""

    val accessKey = vkConfiguration.accessKey
    val securityKey = vkConfiguration.secretKey
    val projectName = vkConfiguration.region
    val iamURL = s"https://iam.${vkConfiguration.region}.myhuaweicloud.com"

    val signedHeaders = "x-hws-date"
    val canonicalHeadersOut = "x-hws-date:" + signTime + "\n"

    val requestBody = """{"auth":{"identity":{"methods":["hw_access_key"],"hw_access_key":{"access":{"key":"""" + accessKey + """"}}},"scope":{"project":{"name":"""" + projectName + """"}}}}"""
    val hexBody = String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(requestBody.toString.getBytes("UTF-8"))))

    val canonicalRequestStr = "POST" + "\n" + "/v3/auth/tokens/" + "\n" + query + "\n" + canonicalHeadersOut + "\n" + signedHeaders + "\n" + hexBody
    val canonicalRequestStrToStr = String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(canonicalRequestStr.toString.getBytes("UTF-8"))))

    val credentialString = time + "/" + region + "/" + service + "/" + terminalString
    val stringToSign =
      s"""${authHeaderPrefix}
         |${signTime}
         |${credentialString}
         |${canonicalRequestStrToStr}""".stripMargin

    // hmac256
    val secret = hwsName + securityKey
    val timeData = getHmacSH256(secret.getBytes("UTF-8"), time.getBytes("UTF-8"))
    val regionData = getHmacSH256(timeData, region.getBytes("UTF-8"))
    val serviceData = getHmacSH256(regionData, service.getBytes("UTF-8"))
    val credentials = getHmacSH256(serviceData, terminalString.getBytes("UTF-8"))
    val toSignature = getHmacSH256(credentials, stringToSign.getBytes("UTF-8"))
    val signature = toSignature.map("%02x" format _).mkString
    val signHeader = authHeaderPrefix + " Credential=" + accessKey + "/" + credentialString + ", " + "SignedHeaders=" +  signedHeaders + ", " + "Signature=" + signature

    // http post
    val httpclient = HttpClients.createDefault()
    val post = new HttpPost(iamURL + "/v3/auth/tokens")
    post.setHeader("X-Identity-Sign", signHeader)
    post.setHeader(hwsDate, signTime)
    post.setHeader("Content-Type", "application/json;charset=utf8")
    post.setEntity(new StringEntity(requestBody, "UTF-8"))
    val response = httpclient.execute(post)
    for (header <- response.getAllHeaders) {
      if (header.getName() == "X-Subject-Token") {
        jobLogger.info("get token succeed.")
        return header.getValue()
      }
    }

    jobLogger.error("get token from IAM failed for: {}", response.getEntity)
    throw new Exception("using ak/sk to get token from IAM Failed.")
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
            case Success(path: Path) if !path.exists =>
              val source = getPath(path.pathAsString.replace(vkJobPaths.workflowPaths.executionRoot.pathAsString, vkConfiguration.storagePath.get)).get
              val future = asyncIo.copyAsync(source, path)
              copies = future :: copies
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
      task => {
//        if(initializationData.restarting) {
//          restartJob(task.name)
//        }
        Future.successful(task)
      })
    jobLogger.warn("taskMessage: {}", taskMessageFuture)
    try {
      for {
        _ <- writeScriptFile()
        taskMessage <- taskMessageFuture
        entity <- Marshal(taskMessage).to[RequestEntity]
        ctr <- makeRequest[Job](HttpRequest(method = HttpMethods.POST,
          headers = List(RawHeader("X-Auth-Token", token)),
          uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs",
          entity = entity))
      } yield PendingExecutionHandle(jobDescriptor, StandardAsyncJob(ctr.name), None, previousState = None)
    } catch {
      case ex: Exception => Future.successful(FailedRetryableExecutionHandle(ex))
      case t: Throwable => throw t
    }
  }

  def restartJob(jobName: String) = {
    pollStatusAsync(jobName) onComplete  {
      case Success(_) => tryAbort(StandardAsyncJob(jobName))
      case Failure(_) => ()
    }
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
      headers = List(RawHeader("X-Auth-Token", token)),
      uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs/${job.jobId}")) onComplete {
      case Success(_) => jobLogger.info("{} Aborted {}", tag: Any, job.jobId)
      case Failure(ex) => jobLogger.warn("{} Failed to abort {}: {}", tag, job.jobId, ex.getMessage)
    }

    ()
  }

  override def requestsAbortAndDiesImmediately: Boolean = false

  override def pollStatusAsync(handle: StandardAsyncPendingExecutionHandle): Future[VkRunStatus] = {
    pollStatusAsync(handle.pendingJob.jobId)
  }

  private def pollStatusAsync(jobName: String): Future[VkRunStatus] = {
    makeRequest[Job](HttpRequest(headers = List(RawHeader("X-Auth-Token", token)),
      uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs/${jobName}")) map {
      response =>
        val state = response.status
        if(state.isEmpty){
          Running
        }else {
          state.get match {
            case s if s.failed.getOrElse(0) > 0 =>
              jobLogger.info(s"VK reported an error for Job ${jobName}: '$s'")
              FailedOrError

            case s if s.succeeded.getOrElse(0) > 0 =>
              jobLogger.info(s"Job ${jobName} is complete")
              Complete

            case s if s.active.getOrElse(1) == 0 =>
              jobLogger.info(s"Job ${jobName} was canceled")
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

  /**
    * Process a successful run, as defined by `isSuccess`.
    *
    * @param runStatus  The run status.
    * @param handle     The execution handle.
    * @param returnCode The return code.
    * @return The execution handle.
    */
  override def handleExecutionSuccess(runStatus: StandardAsyncRunState,
                             handle: StandardAsyncPendingExecutionHandle,
                             returnCode: Int)(implicit ec: ExecutionContext): Future[ExecutionHandle] = {
    super.handleExecutionSuccess(runStatus, handle, returnCode) map {
      case handle: FailedNonRetryableExecutionHandle => FailedRetryableExecutionHandle(handle.throwable)
      case handle: ExecutionHandle => handle
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
      val prePath = vkJobPaths.workflowPaths.executionRoot.pathAsString
      var destPathStr = path.pathAsString
      if(path.pathAsString.startsWith(prePath)){
        destPathStr = path.pathAsString.replace(prePath, vkConfiguration.storagePath.get)
      } else {
        destPathStr = vkConfiguration.storagePath.get + path.pathAsString
      }
      val destPath = getPath(destPathStr).get
      if(vkConfiguration.async) {
        asyncIo.copyAsync(path, destPath)
      } else {
        Await.result(asyncIo.copyAsync(path, destPath), Duration.Inf)
      }
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
