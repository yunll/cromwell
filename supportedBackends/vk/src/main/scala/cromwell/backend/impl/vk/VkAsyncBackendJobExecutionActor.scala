package cromwell.backend.impl.vk

import java.io.FileNotFoundException
import java.nio.file.{FileAlreadyExistsException, Paths}

import cats.syntax.apply._
import akka.stream.ActorMaterializer
import common.validation.ErrorOr.ErrorOr
import common.validation.Validation._
import cromwell.backend.BackendJobLifecycleActor
import cromwell.backend.async.{AbortedExecutionHandle, ExecutionHandle, FailedNonRetryableExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.core.retry.SimpleExponentialBackoff
import wom.values._
import net.ceedubs.ficus.Ficus._
import skuber.api.Configuration
import common.collections.EnhancedCollections._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import skuber.batch.Job
import skuber.k8sInit
import skuber.json.batch.format._
import wdl.draft2.model.FullyQualifiedName

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

  private val kubeConf = Paths.get(vkConfiguration.kubeConf)

  private val cfg = Configuration.parseKubeconfigFile(kubeConf).get.setCurrentNamespace(namespace)

  private val k8s = k8sInit(cfg)

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
      ctr <- k8s.create(taskMessage)
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
    k8s.delete[Job](job.jobId) onComplete {
      case Success(_) => jobLogger.info("{} Aborted {}", tag: Any, job.jobId)
      case Failure(ex) => jobLogger.warn("{} Failed to abort {}: {}", tag, job.jobId, ex.getMessage)
    }

    ()
  }

  override def requestsAbortAndDiesImmediately: Boolean = false

  override def pollStatusAsync(handle: StandardAsyncPendingExecutionHandle): Future[VkRunStatus] = {
    k8s.get[Job](handle.pendingJob.jobId) map {
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

}
