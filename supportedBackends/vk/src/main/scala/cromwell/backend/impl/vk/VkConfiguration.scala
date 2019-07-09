package cromwell.backend.impl.vk

import cromwell.backend.BackendConfigurationDescriptor

class VkConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {
  val apiServerUrl = configurationDescriptor.backendConfig.getString("apiserver-url")
  val namespace = configurationDescriptor.backendConfig.getString("namespace")
  val token = configurationDescriptor.backendConfig.getString("token")
  val storagePath = Option(configurationDescriptor.backendConfig.getString("storagePath"))
  val runtimeConfig = configurationDescriptor.backendRuntimeAttributesConfig
}
