package cromwell.backend.impl.vk


import akka.actor.Actor
import cromwell.backend.standard.StandardCachingActorHelper
import cromwell.core.logging.JobLogging

trait VkJobCachingActorHelper extends StandardCachingActorHelper {
  this: Actor with JobLogging =>

  lazy val initializationData: VkBackendInitializationData = {
    backendInitializationDataAs[VkBackendInitializationData]
  }

  lazy val vkWorkflowPaths: VkWorkflowPaths = workflowPaths.asInstanceOf[VkWorkflowPaths]

  lazy val vkJobPaths: VkJobPaths = jobPaths.asInstanceOf[VkJobPaths]

  lazy val vkConfiguration: VkConfiguration = initializationData.vkConfiguration

  lazy val runtimeAttributes = VkRuntimeAttributes(validatedRuntimeAttributes, vkConfiguration.runtimeConfig)
}
