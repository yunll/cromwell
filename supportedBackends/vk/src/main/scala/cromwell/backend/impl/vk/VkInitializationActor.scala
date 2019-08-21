package cromwell.backend.impl.vk

import akka.actor.ActorRef
import cromwell.backend.standard._
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor}
import cromwell.core.path.PathBuilder
import wom.graph.CommandCallNode

import scala.concurrent.Future

case class VkInitializationActorParams
(
  workflowDescriptor: BackendWorkflowDescriptor,
  calls: Set[CommandCallNode],
  vkConfiguration: VkConfiguration,
  serviceRegistryActor: ActorRef,
  restarting: Boolean,
  vkStatusManager: VkStatusManager
) extends StandardInitializationActorParams {
  override val configurationDescriptor: BackendConfigurationDescriptor = vkConfiguration.configurationDescriptor
}

class VkInitializationActor(params: VkInitializationActorParams)
  extends StandardInitializationActor(params) {

  private val vkConfiguration = params.vkConfiguration
  
  override lazy val pathBuilders: Future[List[PathBuilder]] = {
    standardParams.configurationDescriptor.pathBuildersWithDefault(workflowDescriptor.workflowOptions)
  }

  override lazy val workflowPaths: Future[VkWorkflowPaths] = pathBuilders map {
    new VkWorkflowPaths(workflowDescriptor, vkConfiguration.configurationDescriptor.backendConfig, _)
  }

  override lazy val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder =
    VkRuntimeAttributes.runtimeAttributesBuilder(vkConfiguration.runtimeConfig)

  override def beforeAll(): Future[Option[BackendInitializationData]] = {
    params.vkStatusManager.submit(workflowDescriptor.id.toString, context = context)
    workflowPaths map { paths =>
      publishWorkflowRoot(paths.workflowRoot.toString)
      paths.workflowRoot.createPermissionedDirectories()
      Option(VkBackendInitializationData(paths, runtimeAttributesBuilder, vkConfiguration, params.restarting, params.vkStatusManager))
    }
  }
}
