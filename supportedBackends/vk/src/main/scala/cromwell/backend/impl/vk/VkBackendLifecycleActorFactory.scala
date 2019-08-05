package cromwell.backend.impl.vk

import akka.actor.ActorRef
import cromwell.backend._
import cromwell.backend.standard._
import wom.graph.CommandCallNode

import scala.util.{Success, Try}

case class VkBackendLifecycleActorFactory(name: String, configurationDescriptor: BackendConfigurationDescriptor)
  extends StandardLifecycleActorFactory {

  override lazy val initializationActorClass: Class[_ <: StandardInitializationActor] = classOf[VkInitializationActor]

  override lazy val asyncExecutionActorClass: Class[_ <: StandardAsyncExecutionActor] =
    classOf[VkAsyncBackendJobExecutionActor]

  override def jobIdKey: String = VkAsyncBackendJobExecutionActor.JobIdKey

  val vkConfiguration = new VkConfiguration(configurationDescriptor)

  override def workflowInitializationActorParams(workflowDescriptor: BackendWorkflowDescriptor, ioActor: ActorRef, calls: Set[CommandCallNode],
                                                 serviceRegistryActor: ActorRef, restarting: Boolean): StandardInitializationActorParams = {
    VkInitializationActorParams(workflowDescriptor, calls, vkConfiguration, serviceRegistryActor, restarting)
  }

  override def dockerHashCredentials(workflowDescriptor: BackendWorkflowDescriptor, initializationData: Option[BackendInitializationData]) = {
    Try(BackendInitializationData.as[VkBackendInitializationData](initializationData)) match {
      case Success(data) =>
        List(data.vkConfiguration.accessKey,data.vkConfiguration.secretKey,data.vkConfiguration.region)
      case _ => List.empty[Any]
    }
  }
}
