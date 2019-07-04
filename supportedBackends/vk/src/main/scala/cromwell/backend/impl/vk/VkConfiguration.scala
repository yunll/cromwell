package cromwell.backend.impl.vk

import cromwell.backend.BackendConfigurationDescriptor

class VkConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {
  val kubeConf = configurationDescriptor.backendConfig.getString("kubeConf")
  val namespace = configurationDescriptor.backendConfig.getString("namespace")
  val runtimeConfig = configurationDescriptor.backendRuntimeAttributesConfig
}
