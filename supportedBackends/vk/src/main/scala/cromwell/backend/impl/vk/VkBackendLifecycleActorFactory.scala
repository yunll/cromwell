package cromwell.backend.impl.vk

import akka.actor.ActorRef
import cromwell.backend._
import cromwell.backend.impl.sfs.config.ConfigBackendFileHashingActor
import cromwell.backend.sfs.SharedFileSystemCacheHitCopyingActor
import cromwell.backend.standard._
import cromwell.backend.standard.callcaching.{StandardCacheHitCopyingActor, StandardFileHashingActor}
import cromwell.core.CallOutputs
import wom.graph.CommandCallNode

import scala.util.{Success, Try}

case class VkBackendLifecycleActorFactory(val name: String, val configurationDescriptor: BackendConfigurationDescriptor)
  extends StandardLifecycleActorFactory {

  override lazy val initializationActorClass: Class[_ <: StandardInitializationActor] = classOf[VkInitializationActor]

  override lazy val finalizationActorClassOption: Option[Class[_ <: StandardFinalizationActor]] = Option(classOf[VkFinalizationActor])

  override lazy val asyncExecutionActorClass: Class[_ <: StandardAsyncExecutionActor] =
    classOf[VkAsyncBackendJobExecutionActor]

  override def jobIdKey: String = VkAsyncBackendJobExecutionActor.JobIdKey

  val vkConfiguration = new VkConfiguration(configurationDescriptor)

  val vkStatusManager = new VkStatusManager(vkConfiguration)

  override def workflowInitializationActorParams(workflowDescriptor: BackendWorkflowDescriptor, ioActor: ActorRef, calls: Set[CommandCallNode],
                                                 serviceRegistryActor: ActorRef, restarting: Boolean): StandardInitializationActorParams = {
    VkInitializationActorParams(workflowDescriptor, calls, vkConfiguration, serviceRegistryActor, restarting, vkStatusManager)
  }

  override def workflowFinalizationActorParams(workflowDescriptor: BackendWorkflowDescriptor, ioActor: ActorRef, calls: Set[CommandCallNode],
                                      jobExecutionMap: JobExecutionMap, workflowOutputs: CallOutputs,
                                      initializationDataOption: Option[BackendInitializationData]):
  VkFinalizationActorParams = {
    VkFinalizationActorParams(workflowDescriptor, calls, jobExecutionMap, workflowOutputs,
      initializationDataOption, vkConfiguration, vkStatusManager)
  }

  override def dockerHashCredentials(workflowDescriptor: BackendWorkflowDescriptor, initializationData: Option[BackendInitializationData]) = {
    Try(BackendInitializationData.as[VkBackendInitializationData](initializationData)) match {
      case Success(data) =>
        List(data.vkConfiguration.accessKey,data.vkConfiguration.secretKey,data.vkConfiguration.region)
      case _ => List.empty[Any]
    }
  }

  override lazy val cacheHitCopyingActorClassOption: Option[Class[_ <: StandardCacheHitCopyingActor]] = {
    Option(classOf[SharedFileSystemCacheHitCopyingActor])
  }

  override lazy val fileHashingActorClassOption: Option[Class[_ <: StandardFileHashingActor]] = Option(classOf[ConfigBackendFileHashingActor])

}
