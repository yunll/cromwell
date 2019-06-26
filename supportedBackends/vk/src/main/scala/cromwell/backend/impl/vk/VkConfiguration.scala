package cromwell.backend.impl.vk

import cromwell.backend.BackendConfigurationDescriptor

class VkConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {
  val endpointURL = configurationDescriptor.backendConfig.getString("endpoint")
  val runtimeConfig = configurationDescriptor.backendRuntimeAttributesConfig
}
